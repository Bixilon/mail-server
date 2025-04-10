/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs Ltd <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod dkim;
pub mod dns;
pub mod log;
pub mod principal;
pub mod queue;
pub mod reload;
pub mod report;
pub mod settings;
pub mod spam;
pub mod stores;
pub mod troubleshoot;

use std::{borrow::Cow, str::FromStr, sync::Arc};

use common::{auth::AccessToken, Server};
use directory::{backend::internal::manage, Permission};
use dkim::DkimManagement;
use dns::DnsManagement;
use hyper::Method;
use log::LogManagement;
use mail_parser::DateTime;
use principal::PrincipalManager;
use queue::QueueManagement;
use reload::ManageReload;
use report::ManageReports;
use serde::Serialize;
use settings::ManageSettings;
use spam::ManageSpamHandler;
use store::write::now;
use stores::ManageStore;
use troubleshoot::TroubleshootApi;

use crate::{auth::oauth::auth::OAuthApiHandler, email::crypto::CryptoHandler};

use super::{
    http::{fetch_body, HttpSessionData},
    HttpRequest, HttpResponse,
};
use std::future::Future;

#[derive(Serialize)]
#[serde(tag = "error")]
#[serde(rename_all = "camelCase")]
pub enum ManagementApiError<'x> {
    FieldAlreadyExists {
        field: &'x str,
        value: &'x str,
    },
    FieldMissing {
        field: &'x str,
    },
    NotFound {
        item: &'x str,
    },
    Unsupported {
        details: &'x str,
    },
    AssertFailed,
    Other {
        details: &'x str,
        reason: Option<&'x str>,
    },
}

pub trait ManagementApi: Sync + Send {
    fn handle_api_manage_request(
        &self,
        req: &mut HttpRequest,
        access_token: Arc<AccessToken>,
        session: &HttpSessionData,
    ) -> impl Future<Output = trc::Result<HttpResponse>> + Send;
}

impl ManagementApi for Server {
    #[allow(unused_variables)]
    async fn handle_api_manage_request(
        &self,
        req: &mut HttpRequest,
        access_token: Arc<AccessToken>,
        session: &HttpSessionData,
    ) -> trc::Result<HttpResponse> {
        let body = fetch_body(req, 1024 * 1024, session.session_id).await;
        let path = req.uri().path().split('/').skip(2).collect::<Vec<_>>();

        match path.first().copied().unwrap_or_default() {
            "queue" => self.handle_manage_queue(req, path, &access_token).await,
            "settings" => {
                self.handle_manage_settings(req, path, body, &access_token)
                    .await
            }
            "reports" => self.handle_manage_reports(req, path, &access_token).await,
            "principal" => {
                self.handle_manage_principal(req, path, body, &access_token)
                    .await
            }
            "dns" => self.handle_manage_dns(req, path, &access_token).await,
            "store" => {
                self.handle_manage_store(req, path, body, session, &access_token)
                    .await
            }
            "reload" => self.handle_manage_reload(req, path, &access_token).await,
            "dkim" => {
                self.handle_manage_dkim(req, path, body, &access_token)
                    .await
            }
            "update" => self.handle_manage_update(req, path, &access_token).await,
            "logs" if req.method() == Method::GET => {
                self.handle_view_logs(req, &access_token).await
            }
            "spam-filter" => {
                self.handle_manage_spam(req, path, body, session, &access_token)
                    .await
            }
            "restart" if req.method() == Method::GET => {
                // Validate the access token
                access_token.assert_has_permission(Permission::Restart)?;

                Err(manage::unsupported("Restart is not yet supported"))
            }
            "oauth" => {
                // Validate the access token
                access_token.assert_has_permission(Permission::AuthenticateOauth)?;

                self.handle_oauth_api_request(access_token, body).await
            }
            "account" => match (path.get(1).copied().unwrap_or_default(), req.method()) {
                ("crypto", &Method::POST) => {
                    // Validate the access token
                    access_token.assert_has_permission(Permission::ManageEncryption)?;

                    self.handle_crypto_post(access_token, body).await
                }
                ("crypto", &Method::GET) => {
                    // Validate the access token
                    access_token.assert_has_permission(Permission::ManageEncryption)?;

                    self.handle_crypto_get(access_token).await
                }
                ("auth", &Method::GET) => {
                    // Validate the access token
                    access_token.assert_has_permission(Permission::ManagePasswords)?;

                    self.handle_account_auth_get(access_token).await
                }
                ("auth", &Method::POST) => {
                    // Validate the access token
                    access_token.assert_has_permission(Permission::ManagePasswords)?;

                    self.handle_account_auth_post(req, access_token, body).await
                }
                _ => Err(trc::ResourceEvent::NotFound.into_err()),
            },
            "troubleshoot" => {
                // Validate the access token
                access_token.assert_has_permission(Permission::Troubleshoot)?;

                self.handle_troubleshoot_api_request(req, path, &access_token, body)
                    .await
            }
            _ => Err(trc::ResourceEvent::NotFound.into_err()),
        }
    }
}

pub fn decode_path_element(item: &str) -> Cow<'_, str> {
    // Bit hackish but avoids an extra dependency
    form_urlencoded::parse(item.as_bytes())
        .into_iter()
        .next()
        .map(|(k, _)| k)
        .unwrap_or_else(|| item.into())
}

pub(super) struct FutureTimestamp(u64);
pub(super) struct Timestamp(u64);

impl FromStr for Timestamp {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(dt) = DateTime::parse_rfc3339(s) {
            Ok(Timestamp(dt.to_timestamp() as u64))
        } else {
            Err(())
        }
    }
}

impl FromStr for FutureTimestamp {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(dt) = DateTime::parse_rfc3339(s) {
            let instant = dt.to_timestamp() as u64;
            if instant >= now() {
                return Ok(FutureTimestamp(instant));
            }
        }

        Err(())
    }
}

impl FutureTimestamp {
    pub fn into_inner(self) -> u64 {
        self.0
    }
}

impl Timestamp {
    pub fn into_inner(self) -> u64 {
        self.0
    }
}
