/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs Ltd <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::ops::Range;

use common::{auth::AccessToken, Server};
use jmap_proto::types::{
    acl::Acl,
    blob::{BlobId, BlobSection},
    collection::Collection,
};
use mail_parser::{
    decoders::{base64::base64_decode, quoted_printable::quoted_printable_decode},
    Encoding,
};
use std::future::Future;
use store::BlobClass;
use trc::AddContext;
use utils::BlobHash;

use crate::auth::acl::AclMethods;

pub trait BlobDownload: Sync + Send {
    fn blob_download(
        &self,
        blob_id: &BlobId,
        access_token: &AccessToken,
    ) -> impl Future<Output = trc::Result<Option<Vec<u8>>>> + Send;

    fn get_blob_section(
        &self,
        hash: &BlobHash,
        section: &BlobSection,
    ) -> impl Future<Output = trc::Result<Option<Vec<u8>>>> + Send;

    fn get_blob(
        &self,
        hash: &BlobHash,
        range: Range<usize>,
    ) -> impl Future<Output = trc::Result<Option<Vec<u8>>>> + Send;

    fn has_access_blob(
        &self,
        blob_id: &BlobId,
        access_token: &AccessToken,
    ) -> impl Future<Output = trc::Result<bool>> + Send;
}

impl BlobDownload for Server {
    #[allow(clippy::blocks_in_conditions)]
    async fn blob_download(
        &self,
        blob_id: &BlobId,
        access_token: &AccessToken,
    ) -> trc::Result<Option<Vec<u8>>> {
        if !self
            .core
            .storage
            .data
            .blob_has_access(&blob_id.hash, &blob_id.class)
            .await
            .caused_by(trc::location!())?
        {
            return Ok(None);
        }

        if !access_token.is_member(blob_id.class.account_id()) {
            match &blob_id.class {
                BlobClass::Linked {
                    account_id,
                    collection,
                    document_id,
                } => {
                    if Collection::from(*collection) == Collection::Email {
                        match self
                            .shared_messages(access_token, *account_id, Acl::ReadItems)
                            .await
                        {
                            Ok(shared_messages) if shared_messages.contains(*document_id) => (),
                            _ => return Ok(None),
                        }
                    } else {
                        match self
                            .has_access_to_document(
                                access_token,
                                *account_id,
                                *collection,
                                *document_id,
                                Acl::Read,
                            )
                            .await
                        {
                            Ok(has_access) if has_access => (),
                            _ => return Ok(None),
                        }
                    }
                }
                BlobClass::Reserved { .. } => {
                    return Ok(None);
                }
            }
        }

        if let Some(section) = &blob_id.section {
            self.get_blob_section(&blob_id.hash, section).await
        } else {
            self.get_blob(&blob_id.hash, 0..usize::MAX).await
        }
    }

    async fn get_blob_section(
        &self,
        hash: &BlobHash,
        section: &BlobSection,
    ) -> trc::Result<Option<Vec<u8>>> {
        Ok(self
            .get_blob(
                hash,
                (section.offset_start)..(section.offset_start.saturating_add(section.size)),
            )
            .await?
            .and_then(|bytes| match Encoding::from(section.encoding) {
                Encoding::None => Some(bytes),
                Encoding::Base64 => base64_decode(&bytes),
                Encoding::QuotedPrintable => quoted_printable_decode(&bytes),
            }))
    }

    #[inline(always)]
    async fn get_blob(&self, hash: &BlobHash, range: Range<usize>) -> trc::Result<Option<Vec<u8>>> {
        self.core
            .storage
            .blob
            .get_blob(hash.as_ref(), range)
            .await
            .caused_by(trc::location!())
    }

    async fn has_access_blob(
        &self,
        blob_id: &BlobId,
        access_token: &AccessToken,
    ) -> trc::Result<bool> {
        Ok(self
            .core
            .storage
            .data
            .blob_has_access(&blob_id.hash, &blob_id.class)
            .await
            .caused_by(trc::location!())?
            && match &blob_id.class {
                BlobClass::Linked {
                    account_id,
                    collection,
                    document_id,
                } => {
                    if Collection::from(*collection) == Collection::Email {
                        access_token.is_member(*account_id)
                            || self
                                .shared_messages(access_token, *account_id, Acl::ReadItems)
                                .await?
                                .contains(*document_id)
                    } else {
                        access_token.is_member(*account_id)
                            || (access_token.has_access(*account_id, *collection)
                                && self
                                    .has_access_to_document(
                                        access_token,
                                        *account_id,
                                        *collection,
                                        *document_id,
                                        Acl::Read,
                                    )
                                    .await?)
                    }
                }
                BlobClass::Reserved { account_id, .. } => access_token.is_member(*account_id),
            })
    }
}
