//! Shared surface execution for standalone and elected-peer coordinators.
use super::*;
use rsf_vsr::VsrTransport;
pub(crate) fn sql<T: VsrTransport>(
    envelope: &RequestEnvelope,
    router: &mut VsrBackedFabricRouter<T>,
    sql: &mut DurableSqlEngine,
    output: &mut dyn Write,
) -> RsfResult<DataPlaneResponse> {
    if envelope.body == sql_stream::REQUEST {
        if envelope.operation_kind != WireOperationKind::Read {
            return Err(invalid("SQL streaming requires SELECT"));
        }
        router.confirm_read_quorum()?;
        sql.reconcile(router)?;
        return sql_stream::stream(sql, envelope, router.latest_version().0, output);
    }
    if envelope.body.is_empty() && envelope.operation_kind == WireOperationKind::Read {
        router.confirm_read_quorum()?;
        sql.reconcile(router)?;
        return sql_stream::buffered(sql, envelope);
    }
    let context = if envelope.body.is_empty() {
        None
    } else {
        Some(rsf_sql::SqlSessionContext::decode(&envelope.body)?)
    };
    let request_client = sql_request_client_from_envelope(envelope)?;

    if envelope.operation_kind == WireOperationKind::Read {
        router.confirm_read_quorum()?;
    }
    let execution = if let Some(context) = &context {
        sql.execute_session(
            router,
            PlacementGroupId(1),
            &envelope.principal,
            context,
            &envelope.operation,
            request_client,
        )?
    } else {
        sql.execute(
            router,
            PlacementGroupId(1),
            &envelope.operation,
            request_client,
        )?
    };
    let payload = match &execution.outcome {
        SqlOutcome::Selected { rows } => {
            serde_json::to_vec(&rows.iter().map(|row| &row.columns).collect::<Vec<_>>())
                .map_err(|err| invalid(format!("failed to encode SQL result: {err}")))?
        }
        _ => Vec::new(),
    };
    let response = DataPlaneResponse::Executed {
        protocol: envelope.protocol.as_str(),
        operation_kind: operation_kind_label(envelope.operation_kind),
        surface: "sql",
        version: execution.version.map(|version| version.0),
        bytes: payload.len() as u64,
        payload,
        detail: format!(
            "{}{} replayed={}",
            sql_outcome_detail(&execution.outcome),
            execution
                .read_snapshot
                .map_or_else(String::new, |snapshot| format!(" snapshot={}", snapshot.0)),
            execution.replayed
        ),
    };
    Ok(response)
}

pub(crate) fn kv<T: VsrTransport>(
    envelope: &RequestEnvelope,
    router: &mut VsrBackedFabricRouter<T>,
    kv: &mut DurableKvEngine,
) -> RsfResult<DataPlaneResponse> {
    let command = parse_kv_command(&envelope.operation, &envelope.body)?;

    if matches!(&command, KvCommand::Get { .. }) {
        router.confirm_read_quorum()?;
    }
    match command {
        KvCommand::Get { key } => {
            let value = kv.get(key.as_bytes());
            Ok(DataPlaneResponse::Executed {
                protocol: envelope.protocol.as_str(),
                operation_kind: operation_kind_label(envelope.operation_kind),
                surface: "kv",
                version: value.map(|value| value.version.0),
                bytes: value.map_or(0, |value| value.value.len() as u64),
                payload: value.map_or_else(Vec::new, |value| value.value.clone()),
                detail: match value {
                    Some(value) => format!(
                        "hit key={} version={} deleted={}",
                        key, value.version.0, value.deleted
                    ),
                    None => format!("miss key={key}"),
                },
            })
        }
        KvCommand::Set { key, value } => {
            let request = client_request_from_envelope(envelope)?;
            let snapshot = kv.snapshot(&*router);
            let result = kv.commit_batch_routed(
                router,
                KvBatch {
                    client_request: Some(request),
                    snapshot,
                    mutations: vec![KvMutation::Set {
                        key: key.as_bytes().to_vec(),
                        value: value.clone(),
                    }],
                },
            )?;
            Ok(DataPlaneResponse::Executed {
                protocol: envelope.protocol.as_str(),
                operation_kind: operation_kind_label(envelope.operation_kind),
                surface: "kv",
                version: Some(result.version.0),
                bytes: value.len() as u64,
                payload: Vec::new(),
                detail: format!(
                    "set key={} replayed={} placement_group={} log_index={}",
                    key,
                    result.replayed,
                    result.evidence.placement_group.0,
                    result.evidence.log_index.0
                ),
            })
        }
        KvCommand::Clear { key } => {
            let request = client_request_from_envelope(envelope)?;
            let snapshot = kv.snapshot(&*router);
            let result = kv.commit_batch_routed(
                router,
                KvBatch {
                    client_request: Some(request),
                    snapshot,
                    mutations: vec![KvMutation::Clear {
                        key: key.as_bytes().to_vec(),
                    }],
                },
            )?;
            Ok(DataPlaneResponse::Executed {
                protocol: envelope.protocol.as_str(),
                operation_kind: operation_kind_label(envelope.operation_kind),
                surface: "kv",
                version: Some(result.version.0),
                bytes: 0,
                payload: Vec::new(),
                detail: format!(
                    "clear key={} replayed={} placement_group={} log_index={}",
                    key,
                    result.replayed,
                    result.evidence.placement_group.0,
                    result.evidence.log_index.0
                ),
            })
        }
    }
}

pub(crate) fn blob<T: VsrTransport>(
    envelope: &RequestEnvelope,
    router: &mut VsrBackedFabricRouter<T>,
    blobs: &mut DurableBlobEngine,
) -> RsfResult<DataPlaneResponse> {
    let command = parse_blob_command(&envelope.operation, &envelope.body)?;

    match command {
        BlobCommand::Put { name: _, bytes } => {
            let record = blobs.put_request(
                router,
                PlacementGroupId(1),
                client_request_from_envelope_for_surface(envelope, "blob")?,
                bytes,
            )?;
            Ok(DataPlaneResponse::Executed {
                protocol: envelope.protocol.as_str(),
                operation_kind: operation_kind_label(envelope.operation_kind),
                surface: "blob",
                version: Some(record.pin_version.0),
                bytes: record.bytes,
                payload: Vec::new(),
                detail: format!(
                    "put id=blake3:{} deduped={} pins={} placement_group={} log_index={}",
                    hex_bytes(record.id.as_bytes()),
                    record.deduped,
                    record.pins,
                    record.pin_evidence.placement_group.0,
                    record.pin_evidence.log_index.0
                ),
            })
        }
        BlobCommand::Get { id } => {
            let bytes = blobs.read_verified(&id)?;
            Ok(DataPlaneResponse::Executed {
                protocol: envelope.protocol.as_str(),
                operation_kind: operation_kind_label(envelope.operation_kind),
                surface: "blob",
                version: None,
                bytes: bytes.len() as u64,
                payload: bytes,
                detail: format!("hit id=blake3:{}", hex_bytes(id.as_bytes())),
            })
        }
    }
}

pub(crate) fn object<T: VsrTransport>(
    envelope: &RequestEnvelope,
    router: &mut VsrBackedFabricRouter<T>,
    objects: &mut DurableObjectStore,
) -> RsfResult<DataPlaneResponse> {
    let command = parse_object_command(&envelope.operation, &envelope.body)?;

    if matches!(&command, ObjectCommand::Get { .. }) {
        router.confirm_read_quorum()?;
    }
    match command {
        ObjectCommand::CreateBucket { bucket } => {
            let record = objects.create_bucket_request(
                router,
                PlacementGroupId(1),
                client_request_from_envelope_for_surface(envelope, "object")?,
                bucket,
            )?;
            Ok(DataPlaneResponse::Executed {
                protocol: envelope.protocol.as_str(),
                operation_kind: operation_kind_label(envelope.operation_kind),
                surface: "object",
                version: Some(record.version.0),
                bytes: 0,
                payload: Vec::new(),
                detail: format!(
                    "create_bucket bucket={} placement_group={} log_index={}",
                    record.name, record.evidence.placement_group.0, record.evidence.log_index.0
                ),
            })
        }
        ObjectCommand::Put { bucket, key, bytes } => {
            let request = client_request_from_envelope_for_surface(envelope, "object")?;
            let put = objects.put(
                router,
                PlacementGroupId(1),
                PlacementGroupId(1),
                request,
                bucket,
                key,
                bytes,
            )?;
            Ok(DataPlaneResponse::Executed {
                protocol: envelope.protocol.as_str(),
                operation_kind: operation_kind_label(envelope.operation_kind),
                surface: "object",
                version: Some(put.head.version.0),
                bytes: put.head.bytes,
                payload: Vec::new(),
                detail: format!(
                    "put bucket={} key={} id=blake3:{} manifest=blake3:{} deduped={} pins={} placement_group={} log_index={}",
                    put.head.bucket,
                    put.head.key,
                    hex_bytes(put.head.content.as_bytes()),
                    hex_bytes(put.head.manifest_digest.as_bytes()),
                    put.deduped,
                    put.pins,
                    put.head.evidence.placement_group.0,
                    put.head.evidence.log_index.0
                ),
            })
        }
        ObjectCommand::Get { bucket, key } => {
            let bytes = objects.read_verified(&bucket, &key)?;
            let head = objects
                .head(&bucket, &key)
                .expect("object head exists after verified read");
            Ok(DataPlaneResponse::Executed {
                protocol: envelope.protocol.as_str(),
                operation_kind: operation_kind_label(envelope.operation_kind),
                surface: "object",
                version: Some(head.version.0),
                bytes: bytes.len() as u64,
                payload: bytes,
                detail: format!(
                    "hit bucket={} key={} id=blake3:{} manifest=blake3:{}",
                    bucket,
                    key,
                    hex_bytes(head.content.as_bytes()),
                    hex_bytes(head.manifest_digest.as_bytes())
                ),
            })
        }
    }
}
