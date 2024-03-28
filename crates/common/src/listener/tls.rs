/*
 * Copyright (c) 2023 Stalwart Labs Ltd.
 *
 * This file is part of Stalwart Mail Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use std::{
    cmp::Ordering,
    fmt::{self, Formatter},
    sync::{atomic::AtomicBool, Arc},
};

use ahash::AHashMap;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use rustls::{
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
    version::{TLS12, TLS13},
    SupportedProtocolVersion,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_rustls::{Accept, LazyConfigAcceptor};

use crate::{Core, SharedCore};

use super::{
    acme::{resolver::IsTlsAlpnChallenge, AcmeProvider},
    SessionStream, TcpAcceptor, TcpAcceptorResult,
};

pub static TLS13_VERSION: &[&SupportedProtocolVersion] = &[&TLS13];
pub static TLS12_VERSION: &[&SupportedProtocolVersion] = &[&TLS12];

#[derive(Default)]
pub struct TlsManager {
    pub certificates: ArcSwap<AHashMap<String, Arc<CertifiedKey>>>,
    pub acme_providers: AHashMap<String, AcmeProvider>,
    pub(crate) acme_auth_keys: Mutex<AHashMap<String, AcmeAuthKey>>,
    pub acme_in_progress: AtomicBool,
    pub self_signed_cert: Option<Arc<CertifiedKey>>,
}

pub(crate) struct AcmeAuthKey {
    pub provider_id: String,
    pub key: Arc<CertifiedKey>,
}

#[derive(Clone)]
pub struct CertificateResolver {
    pub core: SharedCore,
}

impl CertificateResolver {
    pub fn new(core: SharedCore) -> Self {
        Self { core }
    }
}

impl AcmeAuthKey {
    pub fn new(provider_id: String, key: Arc<CertifiedKey>) -> Self {
        Self { provider_id, key }
    }
}

impl ResolvesServerCert for CertificateResolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.core
            .as_ref()
            .load()
            .resolve_certificate(hello.server_name())
    }
}

impl Core {
    pub(crate) fn resolve_certificate(&self, name: Option<&str>) -> Option<Arc<CertifiedKey>> {
        let certs = self.tls.certificates.load();

        name.map_or_else(
            || certs.get("*"),
            |name| {
                certs
                    .get(name)
                    .or_else(|| {
                        // Try with a wildcard certificate
                        name.split_once('.')
                            .and_then(|(_, domain)| certs.get(domain))
                    })
                    .or_else(|| {
                        tracing::debug!(
                            context = "tls",
                            event = "not-found",
                            client_name = name,
                            "No SNI certificate found by name, using default."
                        );
                        certs.get("*")
                    })
            },
        )
        .or_else(|| match certs.len().cmp(&1) {
            Ordering::Equal => certs.values().next(),
            Ordering::Greater => {
                tracing::debug!(
                    context = "tls",
                    event = "error",
                    "Multiple certificates available and no default certificate configured."
                );
                certs.values().next()
            }
            Ordering::Less => {
                tracing::warn!(
                    context = "tls",
                    event = "error",
                    "No certificates available, using self-signed."
                );
                self.tls.self_signed_cert.as_ref()
            }
        })
        .cloned()
    }
}

impl TcpAcceptor {
    pub async fn accept<IO>(&self, stream: IO, enable_acme: bool) -> TcpAcceptorResult<IO>
    where
        IO: SessionStream,
    {
        match self {
            TcpAcceptor::Tls {
                acme_config,
                default_config,
                acceptor,
                implicit,
            } if *implicit => {
                if !enable_acme {
                    TcpAcceptorResult::Tls(acceptor.accept(stream))
                } else {
                    match LazyConfigAcceptor::new(Default::default(), stream).await {
                        Ok(start_handshake) => {
                            if start_handshake.client_hello().is_tls_alpn_challenge() {
                                match start_handshake.into_stream(acme_config.clone()).await {
                                    Ok(mut tls) => {
                                        tracing::debug!(
                                            context = "acme",
                                            event = "validation",
                                            "Received TLS-ALPN-01 validation request."
                                        );
                                        let _ = tls.shutdown().await;
                                    }
                                    Err(err) => {
                                        tracing::info!(
                                            context = "acme",
                                            event = "error",
                                            error = ?err,
                                            "TLS-ALPN-01 validation request failed."
                                        );
                                    }
                                }
                            } else {
                                return TcpAcceptorResult::Tls(
                                    start_handshake.into_stream(default_config.clone()),
                                );
                            }
                        }
                        Err(err) => {
                            tracing::debug!(
                                context = "listener",
                                event = "error",
                                error = ?err,
                                "TLS handshake failed."
                            );
                        }
                    }

                    TcpAcceptorResult::Close
                }
            }
            _ => TcpAcceptorResult::Plain(stream),
        }
    }

    pub fn is_tls(&self) -> bool {
        matches!(self, TcpAcceptor::Tls { .. })
    }
}

impl<IO> TcpAcceptorResult<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    pub fn unwrap_tls(self) -> Accept<IO> {
        match self {
            TcpAcceptorResult::Tls(accept) => accept,
            _ => panic!("unwrap_tls called on non-TLS acceptor"),
        }
    }
}

impl std::fmt::Debug for CertificateResolver {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CertificateResolver").finish()
    }
}
