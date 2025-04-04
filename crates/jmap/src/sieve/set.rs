/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs Ltd <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use common::{
    Server,
    auth::{AccessToken, ResourceToken},
};
use jmap_proto::{
    error::set::{SetError, SetErrorType},
    method::set::{SetRequest, SetResponse},
    object::{
        Object,
        index::{IndexAs, IndexProperty, ObjectIndexBuilder},
        sieve::SetArguments,
    },
    request::reference::MaybeReference,
    response::references::EvalObjectReferences,
    types::{
        blob::BlobId,
        collection::Collection,
        id::Id,
        property::Property,
        value::{MaybePatchValue, SetValue, Value},
    },
};
use rand::distr::Alphanumeric;
use sieve::compiler::ErrorType;
use store::{
    BlobClass,
    query::Filter,
    rand::{Rng, rng},
    write::{
        BatchBuilder, BlobOp, DirectoryClass, F_CLEAR, F_VALUE, assert::HashedValue,
        log::ChangeLogBuilder,
    },
};
use trc::AddContext;

use crate::{JmapMethods, api::http::HttpSessionData, blob::download::BlobDownload};
use std::future::Future;

pub struct SetContext<'x> {
    resource_token: ResourceToken,
    access_token: &'x AccessToken,
    response: SetResponse,
}

pub static SCHEMA: &[IndexProperty] = &[
    IndexProperty::new(Property::Name)
        .index_as(IndexAs::Text {
            tokenize: true,
            index: true,
        })
        .max_size(255)
        .required(),
    IndexProperty::new(Property::IsActive).index_as(IndexAs::Integer),
];

pub trait SieveScriptSet: Sync + Send {
    fn sieve_script_set(
        &self,
        request: SetRequest<SetArguments>,
        access_token: &AccessToken,
        session: &HttpSessionData,
    ) -> impl Future<Output = trc::Result<SetResponse>> + Send;

    fn sieve_script_delete(
        &self,
        resource_token: &ResourceToken,
        document_id: u32,
        fail_if_active: bool,
    ) -> impl Future<Output = trc::Result<bool>> + Send;

    #[allow(clippy::type_complexity)]
    fn sieve_set_item(
        &self,
        changes_: Object<SetValue>,
        update: Option<(u32, HashedValue<Object<Value>>)>,
        ctx: &SetContext,
        session_id: u64,
    ) -> impl Future<Output = trc::Result<Result<(ObjectIndexBuilder, Option<Vec<u8>>), SetError>>> + Send;

    fn sieve_activate_script(
        &self,
        account_id: u32,
        activate_id: Option<u32>,
    ) -> impl Future<Output = trc::Result<Vec<(u32, bool)>>> + Send;
}

impl SieveScriptSet for Server {
    async fn sieve_script_set(
        &self,
        mut request: SetRequest<SetArguments>,
        access_token: &AccessToken,
        session: &HttpSessionData,
    ) -> trc::Result<SetResponse> {
        let account_id = request.account_id.document_id();
        let mut sieve_ids = self
            .get_document_ids(account_id, Collection::SieveScript)
            .await?
            .unwrap_or_default();
        let mut ctx = SetContext {
            resource_token: self.get_resource_token(access_token, account_id).await?,
            access_token,
            response: self
                .prepare_set_response(&request, Collection::SieveScript)
                .await?,
        };
        let will_destroy = request.unwrap_destroy();

        // Process creates
        let mut changes = ChangeLogBuilder::new();
        for (id, object) in request.unwrap_create() {
            if sieve_ids.len() as usize <= self.core.jmap.sieve_max_scripts {
                match self
                    .sieve_set_item(object, None, &ctx, session.session_id)
                    .await?
                {
                    Ok((mut builder, Some(blob))) => {
                        // Store blob
                        let blob_id = builder.changes_mut().unwrap().blob_id_mut().unwrap();
                        blob_id.hash = self.put_blob(account_id, &blob, false).await?.hash;
                        let script_size = blob_id.section.as_ref().unwrap().size;
                        let mut blob_id = blob_id.clone();

                        // Write record
                        let mut batch = BatchBuilder::new();
                        batch
                            .with_account_id(account_id)
                            .with_collection(Collection::SieveScript)
                            .create_document()
                            .add(DirectoryClass::UsedQuota(account_id), script_size as i64)
                            .set(
                                BlobOp::Link {
                                    hash: blob_id.hash.clone(),
                                },
                                Vec::new(),
                            )
                            .custom(builder);

                        let document_id = self
                            .store()
                            .write_expect_id(batch)
                            .await
                            .caused_by(trc::location!())?;
                        sieve_ids.insert(document_id);
                        changes.log_insert(Collection::SieveScript, document_id);

                        // Add result with updated blobId
                        blob_id.class = BlobClass::Linked {
                            account_id,
                            collection: Collection::SieveScript.into(),
                            document_id,
                        };
                        ctx.response.created.insert(
                            id,
                            Object::with_capacity(1)
                                .with_property(Property::Id, Value::Id(document_id.into()))
                                .with_property(Property::BlobId, blob_id),
                        );
                    }
                    Err(err) => {
                        ctx.response.not_created.append(id, err);
                    }
                    _ => unreachable!(),
                }
            } else {
                ctx.response.not_created.append(
                    id,
                    SetError::new(SetErrorType::OverQuota).with_description(concat!(
                        "There are too many sieve scripts, ",
                        "please delete some before adding a new one."
                    )),
                );
            }
        }

        // Process updates
        'update: for (id, object) in request.unwrap_update() {
            // Make sure id won't be destroyed
            if will_destroy.contains(&id) {
                ctx.response
                    .not_updated
                    .append(id, SetError::will_destroy());
                continue 'update;
            }

            // Obtain sieve script
            let document_id = id.document_id();
            if let Some(sieve) = self
                .get_property::<HashedValue<Object<Value>>>(
                    account_id,
                    Collection::SieveScript,
                    document_id,
                    Property::Value,
                )
                .await?
            {
                let prev_blob_id = sieve
                    .inner
                    .blob_id()
                    .ok_or_else(|| {
                        trc::StoreEvent::NotFound
                            .into_err()
                            .caused_by(trc::location!())
                            .document_id(document_id)
                    })?
                    .clone();

                match self
                    .sieve_set_item(
                        object,
                        (document_id, sieve).into(),
                        &ctx,
                        session.session_id,
                    )
                    .await?
                {
                    Ok((mut builder, blob)) => {
                        // Prepare write batch
                        let mut batch = BatchBuilder::new();
                        batch
                            .with_account_id(account_id)
                            .with_collection(Collection::SieveScript)
                            .update_document(document_id);

                        let blob_id = if let Some(blob) = blob {
                            // Store blob
                            let blob_id = builder.changes_mut().unwrap().blob_id_mut().unwrap();
                            blob_id.hash = self.put_blob(account_id, &blob, false).await?.hash;
                            let script_size = blob_id.section.as_ref().unwrap().size as i64;
                            let prev_script_size =
                                prev_blob_id.section.as_ref().unwrap().size as i64;
                            let blob_id = blob_id.clone();

                            // Update quota
                            let update_quota = match script_size.cmp(&prev_script_size) {
                                std::cmp::Ordering::Greater => script_size - prev_script_size,
                                std::cmp::Ordering::Less => -prev_script_size + script_size,
                                std::cmp::Ordering::Equal => 0,
                            };
                            if update_quota != 0 {
                                batch.add(DirectoryClass::UsedQuota(account_id), update_quota);
                            }

                            // Update blobId
                            batch
                                .clear(BlobOp::Link {
                                    hash: prev_blob_id.hash,
                                })
                                .set(
                                    BlobOp::Link {
                                        hash: blob_id.hash.clone(),
                                    },
                                    Vec::new(),
                                );

                            blob_id.into()
                        } else {
                            None
                        };

                        // Write record
                        batch.custom(builder);

                        if !batch.is_empty() {
                            changes.log_update(Collection::SieveScript, document_id);
                            match self.core.storage.data.write(batch.build()).await {
                                Ok(_) => (),
                                Err(err) if err.is_assertion_failure() => {
                                    ctx.response.not_updated.append(id, SetError::forbidden().with_description(
                                        "Another process modified this sieve, please try again.",
                                    ));
                                    continue 'update;
                                }
                                Err(err) => {
                                    return Err(err.caused_by(trc::location!()));
                                }
                            }
                        }

                        // Add result with updated blobId
                        ctx.response.updated.append(
                            id,
                            blob_id.map(|blob_id| {
                                Object::with_capacity(1).with_property(Property::BlobId, blob_id)
                            }),
                        );
                    }
                    Err(err) => {
                        ctx.response.not_updated.append(id, err);
                        continue 'update;
                    }
                }
            } else {
                ctx.response.not_updated.append(id, SetError::not_found());
            }
        }

        // Process deletions
        for id in will_destroy {
            let document_id = id.document_id();
            if sieve_ids.contains(document_id) {
                if self
                    .sieve_script_delete(&ctx.resource_token, document_id, true)
                    .await?
                {
                    changes.log_delete(Collection::SieveScript, document_id);
                    ctx.response.destroyed.push(id);
                } else {
                    ctx.response.not_destroyed.append(
                        id,
                        SetError::new(SetErrorType::ScriptIsActive)
                            .with_description("Deactivate Sieve script before deletion."),
                    );
                }
            } else {
                ctx.response.not_destroyed.append(id, SetError::not_found());
            }
        }

        // Activate / deactivate scripts
        if ctx.response.not_created.is_empty()
            && ctx.response.not_updated.is_empty()
            && ctx.response.not_destroyed.is_empty()
            && (request.arguments.on_success_activate_script.is_some()
                || request
                    .arguments
                    .on_success_deactivate_script
                    .unwrap_or(false))
        {
            let changed_ids = if let Some(id) = request.arguments.on_success_activate_script {
                self.sieve_activate_script(
                    account_id,
                    match id {
                        MaybeReference::Value(id) => id.document_id(),
                        MaybeReference::Reference(id_ref) => match ctx.response.get_id(&id_ref) {
                            Some(Value::Id(id)) => id.document_id(),
                            _ => return Ok(ctx.response),
                        },
                    }
                    .into(),
                )
                .await?
            } else {
                self.sieve_activate_script(account_id, None).await?
            };

            for (document_id, is_active) in changed_ids {
                if let Some(obj) = ctx.response.get_object_by_id(Id::from(document_id)) {
                    obj.append(Property::IsActive, Value::Bool(is_active));
                }
                changes.log_update(Collection::SieveScript, document_id);
            }
        }

        // Write changes
        if !changes.is_empty() {
            ctx.response.new_state = Some(self.commit_changes(account_id, changes).await?.into());
        }

        Ok(ctx.response)
    }

    async fn sieve_script_delete(
        &self,
        resource_token: &ResourceToken,
        document_id: u32,
        fail_if_active: bool,
    ) -> trc::Result<bool> {
        // Fetch record
        let account_id = resource_token.account_id;
        let obj = self
            .get_property::<HashedValue<Object<Value>>>(
                account_id,
                Collection::SieveScript,
                document_id,
                Property::Value,
            )
            .await?
            .ok_or_else(|| {
                trc::StoreEvent::NotFound
                    .into_err()
                    .caused_by(trc::location!())
                    .document_id(document_id)
            })?;

        // Make sure the script is not active
        if fail_if_active
            && matches!(
                obj.inner.properties.get(&Property::IsActive),
                Some(Value::Bool(true))
            )
        {
            return Ok(false);
        }

        // Delete record
        let mut batch = BatchBuilder::new();
        let blob_id = obj.inner.blob_id().ok_or_else(|| {
            trc::StoreEvent::NotFound
                .into_err()
                .caused_by(trc::location!())
                .document_id(document_id)
        })?;
        let updated_quota = -(blob_id.section.as_ref().unwrap().size as i64);
        batch
            .with_account_id(account_id)
            .with_collection(Collection::SieveScript)
            .delete_document(document_id)
            .value(Property::EmailIds, (), F_VALUE | F_CLEAR)
            .clear(BlobOp::Link {
                hash: blob_id.hash.clone(),
            })
            .add(DirectoryClass::UsedQuota(account_id), updated_quota)
            .custom(ObjectIndexBuilder::new(SCHEMA).with_current(obj));

        self.store()
            .write(batch)
            .await
            .caused_by(trc::location!())?;
        Ok(true)
    }

    #[allow(clippy::blocks_in_conditions)]
    async fn sieve_set_item(
        &self,
        changes_: Object<SetValue>,
        update: Option<(u32, HashedValue<Object<Value>>)>,
        ctx: &SetContext<'_>,
        session_id: u64,
    ) -> trc::Result<Result<(ObjectIndexBuilder, Option<Vec<u8>>), SetError>> {
        // Vacation script cannot be modified
        if matches!(update.as_ref().and_then(|(_, obj)| obj.inner.properties.get(&Property::Name)), Some(Value::Text ( value )) if value.eq_ignore_ascii_case("vacation"))
        {
            return Ok(Err(SetError::forbidden().with_description(concat!(
                "The 'vacation' script cannot be modified, ",
                "use VacationResponse/set instead."
            ))));
        }

        // Parse properties
        let mut changes = Object::with_capacity(changes_.properties.len());
        let mut blob_id = None;
        for (property, value) in changes_.properties {
            let value = match ctx.response.eval_object_references(value) {
                Ok(value) => value,
                Err(err) => {
                    return Ok(Err(err));
                }
            };
            let value = match (&property, value) {
                (Property::Name, MaybePatchValue::Value(Value::Text(value))) => {
                    if value.len() > self.core.jmap.sieve_max_script_name {
                        return Ok(Err(SetError::invalid_properties()
                            .with_property(property)
                            .with_description("Script name is too long.")));
                    } else if value.eq_ignore_ascii_case("vacation") {
                        return Ok(Err(SetError::forbidden()
                            .with_property(property)
                            .with_description(
                                "The 'vacation' name is reserved, please use a different name.",
                            )));
                    } else if update
                        .as_ref()
                        .and_then(|(_, obj)| obj.inner.properties.get(&Property::Name))
                        .is_none_or(
                            |p| matches!(p, Value::Text (prev_value ) if prev_value != &value),
                        )
                    {
                        if let Some(id) = self
                            .filter(
                                ctx.resource_token.account_id,
                                Collection::SieveScript,
                                vec![Filter::eq(Property::Name, &value)],
                            )
                            .await?
                            .results
                            .min()
                        {
                            return Ok(Err(SetError::already_exists()
                                .with_existing_id(id.into())
                                .with_description(format!(
                                    "A sieve script with name '{}' already exists.",
                                    value
                                ))));
                        }
                    }

                    Value::Text(value)
                }
                (Property::BlobId, MaybePatchValue::Value(Value::BlobId(value))) => {
                    blob_id = value.into();
                    continue;
                }
                (Property::Name, MaybePatchValue::Value(Value::Null)) => {
                    continue;
                }
                _ => {
                    return Ok(Err(SetError::invalid_properties()
                        .with_property(property)
                        .with_description("Invalid property or value.".to_string())));
                }
            };
            changes.append(property, value);
        }

        if update.is_none() {
            // Add name if missing
            if !matches!(changes.properties.get(&Property::Name), Some(Value::Text ( value )) if !value.is_empty())
            {
                changes.set(
                    Property::Name,
                    Value::Text(
                        rng()
                            .sample_iter(Alphanumeric)
                            .take(15)
                            .map(char::from)
                            .collect::<String>(),
                    ),
                );
            }

            // Set script as inactive
            changes.set(Property::IsActive, Value::Bool(false));
        }

        let blob_update = if let Some(blob_id) = blob_id {
            if update.as_ref().is_none_or( |(document_id, _)| {
                !matches!(blob_id.class, BlobClass::Linked { account_id, collection, document_id: d } if account_id == ctx.resource_token.account_id && collection == u8::from(Collection::SieveScript) && *document_id == d)
            }) {
                // Check access
                if let Some(mut bytes) = self.blob_download(&blob_id, ctx.access_token).await? {
                    // Check quota
                    match self
                        .has_available_quota(&ctx.resource_token, bytes.len() as u64)
                        .await
                    {
                        Ok(_) => (),
                        Err(err) => {
                            if err.matches(trc::EventType::Limit(trc::LimitEvent::Quota))
                                || err.matches(trc::EventType::Limit(trc::LimitEvent::TenantQuota))
                            {
                                trc::error!(err.account_id(ctx.resource_token.account_id).span_id(session_id));
                                return Ok(Err(SetError::over_quota()));
                            } else {
                                return Err(err);
                            }
                        }
                    }

                    // Compile script
                    match self.core.sieve.untrusted_compiler.compile(&bytes) {
                        Ok(script) => {
                            changes.set(
                                Property::BlobId,
                                BlobId::default().with_section_size(bytes.len()),
                            );
                            bytes.extend(bincode::serialize(&script).unwrap_or_default());
                            bytes.into()
                        }
                        Err(err) => {
                            return Ok(Err(SetError::new(
                                if let ErrorType::ScriptTooLong = &err.error_type() {
                                    SetErrorType::TooLarge
                                } else {
                                    SetErrorType::InvalidScript
                                },
                            )
                            .with_description(err.to_string())));
                        }
                    }
                } else {
                    return Ok(Err(SetError::new(SetErrorType::BlobNotFound)
                        .with_property(Property::BlobId)
                        .with_description("Blob does not exist.")));
                }
            } else {
                None
            }
        } else if update.is_none() {
            return Ok(Err(SetError::invalid_properties()
                .with_property(Property::BlobId)
                .with_description("Missing blobId.")));
        } else {
            None
        };

        // Validate
        Ok(ObjectIndexBuilder::new(SCHEMA)
            .with_changes(changes)
            .with_current_opt(update.map(|(_, current)| current))
            .validate()
            .map(|obj| (obj, blob_update)))
    }

    async fn sieve_activate_script(
        &self,
        account_id: u32,
        mut activate_id: Option<u32>,
    ) -> trc::Result<Vec<(u32, bool)>> {
        let mut changed_ids = Vec::new();
        // Find the currently active script
        let mut active_ids = self
            .filter(
                account_id,
                Collection::SieveScript,
                vec![Filter::eq(Property::IsActive, 1u32)],
            )
            .await?
            .results;

        // Check if script is already active
        if activate_id.is_some_and(|id| active_ids.remove(id)) {
            if active_ids.is_empty() {
                return Ok(changed_ids);
            } else {
                activate_id = None;
            }
        }

        // Prepare batch
        let mut batch = BatchBuilder::new();
        batch
            .with_account_id(account_id)
            .with_collection(Collection::SieveScript);

        // Deactivate scripts
        for document_id in active_ids {
            if let Some(sieve) = self
                .get_property::<HashedValue<Object<Value>>>(
                    account_id,
                    Collection::SieveScript,
                    document_id,
                    Property::Value,
                )
                .await?
            {
                batch
                    .update_document(document_id)
                    .value(Property::EmailIds, (), F_VALUE | F_CLEAR)
                    .custom(
                        ObjectIndexBuilder::new(SCHEMA)
                            .with_changes(
                                Object::with_capacity(1).with_property(Property::IsActive, false),
                            )
                            .with_current(sieve),
                    );
                changed_ids.push((document_id, false));
            }
        }

        // Activate script
        if let Some(document_id) = activate_id {
            if let Some(sieve) = self
                .get_property::<HashedValue<Object<Value>>>(
                    account_id,
                    Collection::SieveScript,
                    document_id,
                    Property::Value,
                )
                .await?
            {
                batch.update_document(document_id).custom(
                    ObjectIndexBuilder::new(SCHEMA)
                        .with_changes(
                            Object::with_capacity(1).with_property(Property::IsActive, true),
                        )
                        .with_current(sieve),
                );
                changed_ids.push((document_id, true));
            }
        }

        // Write changes
        if !changed_ids.is_empty() {
            match self.core.storage.data.write(batch.build()).await {
                Ok(_) => (),
                Err(err) if err.is_assertion_failure() => {
                    return Ok(vec![]);
                }
                Err(err) => {
                    return Err(err.caused_by(trc::location!()));
                }
            }
        }

        Ok(changed_ids)
    }
}

pub trait ObjectBlobId {
    fn blob_id(&self) -> Option<&BlobId>;
    fn blob_id_mut(&mut self) -> Option<&mut BlobId>;
}

impl ObjectBlobId for Object<Value> {
    fn blob_id(&self) -> Option<&BlobId> {
        self.properties
            .get(&Property::BlobId)
            .and_then(|v| v.as_blob_id())
    }

    fn blob_id_mut(&mut self) -> Option<&mut BlobId> {
        self.properties
            .get_mut(&Property::BlobId)
            .and_then(|v| match v {
                Value::BlobId(blob_id) => Some(blob_id),
                _ => None,
            })
    }
}
