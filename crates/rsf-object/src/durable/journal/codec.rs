use super::*;

pub(super) fn encode_intent(intent: &Intent) -> String {
    let client = hex_bytes(intent.request.client.0.as_bytes());
    let request = intent.request.request.0;
    match &intent.action {
        Action::Bucket { pg, bucket } => format!(
            "intent\tbucket\t{client}\t{request}\t{}\t{}",
            pg.0,
            hex_bytes(bucket.as_bytes())
        ),
        Action::Put {
            pg,
            blob_pg,
            bucket,
            key,
            content,
            bytes,
            deduped,
        } => format!(
            "intent\tput\t{client}\t{request}\t{}\t{}\t{}\t{}\t{}\t{bytes}\t{}",
            pg.0,
            blob_pg.0,
            hex_bytes(bucket.as_bytes()),
            hex_bytes(key.as_bytes()),
            hex_bytes(content.as_bytes()),
            u8::from(*deduped)
        ),
    }
}
fn request(client: &str, id: &str) -> RsfResult<ClientRequestId> {
    let client = decode_string(client, "request client")?;
    if client.is_empty() {
        return Err(corrupt("empty object request client"));
    }
    Ok(ClientRequestId::new(client, parse_u64(id, "request id")?))
}
fn boolean(value: &str) -> RsfResult<bool> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(corrupt("invalid object boolean")),
    }
}
pub(super) fn decode_intent(text: &str) -> RsfResult<Intent> {
    let fields = text.split('\t').collect::<Vec<_>>();
    let (request, action) = match fields.as_slice() {
        ["intent", "bucket", client, id, pg, bucket] => (
            request(client, id)?,
            Action::Bucket {
                pg: PlacementGroupId(parse_u64(pg, "pg")?),
                bucket: decode_string(bucket, "bucket")?,
            },
        ),
        [
            "intent",
            "put",
            client,
            id,
            pg,
            blob_pg,
            bucket,
            key,
            content,
            bytes,
            deduped,
        ] => (
            request(client, id)?,
            Action::Put {
                pg: PlacementGroupId(parse_u64(pg, "pg")?),
                blob_pg: PlacementGroupId(parse_u64(blob_pg, "blob pg")?),
                bucket: decode_string(bucket, "bucket")?,
                key: decode_string(key, "key")?,
                content: ContentId::from_blake3(hex_decode_32(content)?)?,
                bytes: parse_u64(bytes, "bytes")?,
                deduped: boolean(deduped)?,
            },
        ),
        _ => return Err(corrupt("malformed object intent")),
    };
    Ok(Intent { request, action })
}
pub(super) fn encode_receipt(request: &ClientRequestId, receipt: &Receipt) -> String {
    let client = hex_bytes(request.client.0.as_bytes());
    let id = request.request.0;
    match receipt {
        Receipt::Bucket(bucket) => format!(
            "receipt\tbucket\t{client}\t{id}\t{}\t{}",
            hex_bytes(bucket.name.as_bytes()),
            encode_evidence(&bucket.evidence)
        ),
        Receipt::Put(put) => {
            let head = &put.head;
            format!(
                "receipt\tput\t{client}\t{id}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                hex_bytes(head.bucket.as_bytes()),
                hex_bytes(head.key.as_bytes()),
                hex_bytes(head.content.as_bytes()),
                head.bytes,
                hex_bytes(head.manifest_digest.as_bytes()),
                encode_evidence(&head.evidence),
                encode_evidence(&put.pin_evidence),
                u8::from(put.deduped),
                put.pins
            )
        }
    }
}
pub(super) fn decode_receipt(text: &str) -> RsfResult<(ClientRequestId, Receipt)> {
    let fields = text.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        ["receipt", "bucket", client, id, bucket, evidence] => {
            let evidence = decode_evidence(evidence)?;
            Ok((
                request(client, id)?,
                Receipt::Bucket(DurableObjectBucket {
                    name: decode_string(bucket, "bucket")?,
                    version: evidence.version,
                    evidence,
                }),
            ))
        }
        [
            "receipt",
            "put",
            client,
            id,
            bucket,
            key,
            content,
            bytes,
            manifest,
            evidence,
            pin,
            deduped,
            pins,
        ] => {
            let evidence = decode_evidence(evidence)?;
            let pin_evidence = decode_evidence(pin)?;
            Ok((
                request(client, id)?,
                Receipt::Put(DurableObjectPut {
                    head: DurableObjectHead {
                        bucket: decode_string(bucket, "bucket")?,
                        key: decode_string(key, "key")?,
                        content: ContentId::from_blake3(hex_decode_32(content)?)?,
                        bytes: parse_u64(bytes, "bytes")?,
                        manifest_digest: Digest::blake3(hex_decode_32(manifest)?)?,
                        version: evidence.version,
                        evidence,
                    },
                    pin_version: pin_evidence.version,
                    pin_evidence,
                    deduped: boolean(deduped)?,
                    pins: u32::try_from(parse_u64(pins, "pins")?)
                        .map_err(|_| corrupt("object pins exceed u32"))?,
                }),
            ))
        }
        _ => Err(corrupt("malformed object receipt")),
    }
}
