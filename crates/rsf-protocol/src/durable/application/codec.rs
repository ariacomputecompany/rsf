use super::*;
const MAGIC: &[u8; 5] = b"RSFC1";

pub(super) fn encode(command: &ReplicatedFabricCommand) -> RsfResult<Vec<u8>> {
    validate(command)?;
    let mut out = MAGIC.to_vec();
    field(
        &mut out,
        &ProtocolCommitRecord::from_client_request(&command.commit, command.request.clone())
            .encode(),
    )?;
    encode_operation(&mut out, &command.commit.operation)?;
    field(&mut out, command.application.namespace.as_bytes())?;
    out.push(match command.application.surface {
        ApplicationSurface::Kv => 1,
        ApplicationSurface::Sql => 2,
        ApplicationSurface::Object => 3,
        ApplicationSurface::Blob => 4,
    });
    field(&mut out, &command.application.bytes)?;
    if out.len() > rsf_vsr::MAX_REPLICATED_COMMAND_BYTES {
        return Err(invalid(
            "complete application command exceeds the VSR size limit",
        ));
    }
    Ok(out)
}
fn validate(command: &ReplicatedFabricCommand) -> RsfResult<()> {
    let namespace = &command.application.namespace;
    if namespace.is_empty()
        || namespace.len() > 4096
        || namespace.chars().any(char::is_control)
        || command.request.client.0.is_empty()
        || command.request.client.0.len() > 4096
        || command.application.bytes.is_empty()
        || command.application.bytes.len() > rsf_vsr::MAX_REPLICATED_COMMAND_BYTES
        || command.commit.index.0 == 0
        || command.commit.version.0 == 0
        || command.commit.index.0 == u64::MAX
        || command.commit.version.0 == u64::MAX
        || command.commit.operation_id != command.commit.operation.operation_id()
        || (command.commit.mode == ConsistencyMode::AvailabilityFirst
            && command.commit.operation.requires_strict())
    {
        return Err(invalid(
            "invalid complete application command metadata or payload",
        ));
    }
    Ok(())
}
fn field(out: &mut Vec<u8>, value: &[u8]) -> RsfResult<()> {
    if value.len() > rsf_vsr::MAX_REPLICATED_COMMAND_BYTES
        || out.len().saturating_add(4).saturating_add(value.len())
            > rsf_vsr::MAX_REPLICATED_COMMAND_BYTES
    {
        return Err(invalid("application command field exceeds size limit"));
    }
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}
fn encode_operation(out: &mut Vec<u8>, operation: &FabricOperation) -> RsfResult<()> {
    match operation {
        FabricOperation::CatalogMutation { namespace, item } => {
            out.push(1);
            field(out, namespace.as_bytes())?;
            field(out, item.as_bytes())?;
        }
        FabricOperation::TransactionCommit { transaction } => {
            out.push(2);
            out.extend_from_slice(transaction);
        }
        FabricOperation::RelationalCatalog { table, item } => {
            out.push(3);
            field(out, table.as_bytes())?;
            field(out, item.as_bytes())?;
        }
        FabricOperation::RelationalMutation { table, key } => {
            out.push(4);
            field(out, table.as_bytes())?;
            field(out, key)?;
        }
        FabricOperation::KvMutation { key } => {
            out.push(5);
            field(out, key)?;
        }
        FabricOperation::KvBatch { keys } => {
            out.push(6);
            if keys.len() > rsf_vsr::MAX_REPLICATED_COMMAND_BYTES / 4 {
                return Err(invalid("application key count exceeds limit"));
            }
            out.extend_from_slice(&(keys.len() as u32).to_le_bytes());
            for key in keys {
                field(out, key)?;
            }
        }
        FabricOperation::ObjectBucket { bucket } => {
            out.push(7);
            field(out, bucket.as_bytes())?;
        }
        FabricOperation::ObjectMultipartStart {
            bucket,
            key,
            upload_id,
        } => {
            out.push(8);
            field(out, bucket.as_bytes())?;
            field(out, key.as_bytes())?;
            out.extend_from_slice(&upload_id.to_le_bytes());
        }
        FabricOperation::ObjectMultipartPart {
            bucket,
            key,
            upload_id,
            part_number,
        } => {
            out.push(9);
            field(out, bucket.as_bytes())?;
            field(out, key.as_bytes())?;
            out.extend_from_slice(&upload_id.to_le_bytes());
            out.extend_from_slice(&part_number.to_le_bytes());
        }
        FabricOperation::ObjectHead { bucket, key } => {
            out.push(10);
            field(out, bucket.as_bytes())?;
            field(out, key.as_bytes())?;
        }
        FabricOperation::ObjectManifest { bucket, key } => {
            out.push(11);
            field(out, bucket.as_bytes())?;
            field(out, key.as_bytes())?;
        }
        FabricOperation::BlobPin { digest } => {
            out.push(12);
            out.extend_from_slice(digest);
        }
    }
    Ok(())
}
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> RsfResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| corrupt("application command length overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| corrupt("truncated application command"))?;
        self.position = end;
        Ok(value)
    }
    fn byte(&mut self) -> RsfResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> RsfResult<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> RsfResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn field(&mut self) -> RsfResult<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }
    fn string(&mut self) -> RsfResult<String> {
        String::from_utf8(self.field()?.to_vec())
            .map_err(|_| corrupt("application string is not UTF-8"))
    }
    fn operation(&mut self) -> RsfResult<FabricOperation> {
        Ok(match self.byte()? {
            1 => FabricOperation::CatalogMutation {
                namespace: self.string()?,
                item: self.string()?,
            },
            2 => FabricOperation::TransactionCommit {
                transaction: self.take(16)?.try_into().unwrap(),
            },
            3 => FabricOperation::RelationalCatalog {
                table: self.string()?,
                item: self.string()?,
            },
            4 => FabricOperation::RelationalMutation {
                table: self.string()?,
                key: self.field()?.to_vec(),
            },
            5 => FabricOperation::KvMutation {
                key: self.field()?.to_vec(),
            },
            6 => {
                let count = self.u32()? as usize;
                if count > (self.bytes.len() - self.position) / 4 {
                    return Err(corrupt("application key count exceeds remaining bytes"));
                }
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    keys.push(self.field()?.to_vec());
                }
                FabricOperation::KvBatch { keys }
            }
            7 => FabricOperation::ObjectBucket {
                bucket: self.string()?,
            },
            8 => FabricOperation::ObjectMultipartStart {
                bucket: self.string()?,
                key: self.string()?,
                upload_id: self.u64()?,
            },
            9 => {
                let bucket = self.string()?;
                let key = self.string()?;
                let upload_id = self.u64()?;
                let part_number = u16::from_le_bytes(self.take(2)?.try_into().unwrap());
                FabricOperation::ObjectMultipartPart {
                    bucket,
                    key,
                    upload_id,
                    part_number,
                }
            }
            10 => FabricOperation::ObjectHead {
                bucket: self.string()?,
                key: self.string()?,
            },
            11 => FabricOperation::ObjectManifest {
                bucket: self.string()?,
                key: self.string()?,
            },
            12 => FabricOperation::BlobPin {
                digest: self.take(32)?.try_into().unwrap(),
            },
            _ => return Err(corrupt("unknown application operation tag")),
        })
    }
}
pub(super) fn decode(bytes: &[u8]) -> RsfResult<ReplicatedFabricCommand> {
    if bytes.len() > rsf_vsr::MAX_REPLICATED_COMMAND_BYTES {
        return Err(corrupt("application command exceeds size limit"));
    }
    let mut reader = Reader { bytes, position: 0 };
    if reader.take(5)? != MAGIC {
        return Err(corrupt("unknown application command format"));
    }
    let record = ProtocolCommitRecord::decode(reader.field()?)?;
    let operation = reader.operation()?;
    let namespace = reader.string()?;
    let surface = match reader.byte()? {
        1 => ApplicationSurface::Kv,
        2 => ApplicationSurface::Sql,
        3 => ApplicationSurface::Object,
        4 => ApplicationSurface::Blob,
        _ => return Err(corrupt("unknown application surface")),
    };
    let payload = reader.field()?.to_vec();
    if reader.position != bytes.len() {
        return Err(corrupt("application command has trailing bytes"));
    }
    let certificate = record.certificate;
    let command = ReplicatedFabricCommand {
        commit: FabricCommit {
            pg: certificate.pg,
            index: certificate.index,
            version: certificate.version,
            mode: certificate.mode,
            operation_id: certificate.operation_id,
            operation,
        },
        request: record
            .client_request
            .ok_or_else(|| corrupt("application command has no request identity"))?,
        application: ApplicationPayload {
            namespace,
            surface,
            bytes: payload,
        },
    };
    validate(&command).map_err(|err| corrupt(err.message))?;
    Ok(command)
}
