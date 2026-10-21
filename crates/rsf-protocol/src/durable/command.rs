//! Complete non-application operations share the same ordered replication log.
use super::*;
use application::codec::{Reader, encode_operation, field};

pub(super) enum PendingCommand {
    Application(ReplicatedFabricCommand),
    Protocol(ReplicatedProtocolCommand),
}
impl PendingCommand {
    pub(super) fn version(&self) -> CommitVersion {
        match self {
            Self::Application(command) => command.commit.version,
            Self::Protocol(command) => command.commit.version,
        }
    }
}

const MAGIC: &[u8; 5] = b"RSFG1";
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicatedProtocolCommand {
    pub commit: FabricCommit,
    pub request: Option<ClientRequestId>,
}
impl ReplicatedProtocolCommand {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        if self.commit.index.0 == 0
            || self.commit.index.0 == u64::MAX
            || self.commit.version.0 == 0
            || self.commit.version.0 == u64::MAX
            || self.commit.operation_id != self.commit.operation.operation_id()
            || (self.commit.mode == ConsistencyMode::AvailabilityFirst
                && self.commit.operation.requires_strict())
            || self
                .request
                .as_ref()
                .is_some_and(|r| r.client.0.is_empty() || r.client.0.len() > 4096)
        {
            return Err(invalid("invalid replicated protocol command"));
        }
        let record = match &self.request {
            Some(request) => {
                ProtocolCommitRecord::from_client_request(&self.commit, request.clone())
            }
            None => ProtocolCommitRecord::from_commit(&self.commit),
        };
        let mut bytes = MAGIC.to_vec();
        field(&mut bytes, &record.encode())?;
        encode_operation(&mut bytes, &self.commit.operation)?;
        if bytes.len() > rsf_vsr::MAX_REPLICATED_COMMAND_BYTES {
            return Err(invalid("replicated protocol command exceeds limit"));
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() > rsf_vsr::MAX_REPLICATED_COMMAND_BYTES {
            return Err(corrupt("replicated protocol command exceeds limit"));
        }
        let mut reader = Reader { bytes, position: 0 };
        if reader.take(5)? != MAGIC {
            return Err(corrupt("unknown replicated protocol command format"));
        }
        let record = ProtocolCommitRecord::decode(reader.field()?)?;
        let operation = reader.operation()?;
        if reader.position != bytes.len() {
            return Err(corrupt("protocol command has trailing bytes"));
        }
        let certificate = record.certificate;
        let command = Self {
            commit: FabricCommit {
                pg: certificate.pg,
                index: certificate.index,
                version: certificate.version,
                mode: certificate.mode,
                operation_id: certificate.operation_id,
                operation,
            },
            request: record.client_request,
        };
        if command.encode().map_err(|err| corrupt(&err.message))? != bytes {
            return Err(corrupt("noncanonical replicated protocol command"));
        }
        Ok(command)
    }
    pub(super) fn is_payload(bytes: &[u8]) -> bool {
        bytes.starts_with(MAGIC)
    }
    pub(super) fn vsr_operation(&self) -> RsfResult<VsrOperation> {
        Ok(VsrOperation::ReplicatedCommand {
            operation_id: self.commit.operation_id,
            payload: self.encode()?,
        })
    }
}
fn invalid(message: &str) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}
fn corrupt(message: &str) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

impl<T: VsrTransport> VsrBackedFabricRouter<T> {
    pub(super) fn finish_protocol_command(
        &mut self,
        command: &ReplicatedProtocolCommand,
    ) -> RsfResult<()> {
        let next = self.router.next_commit(
            command.commit.pg,
            command.commit.mode,
            command.commit.operation.clone(),
        )?;
        if next != command.commit {
            return Err(corrupt(
                "protocol command would change its original version or index",
            ));
        }
        self.require_vsr_commit(&command.commit, command.request.clone())?;
        self.sync_protocol_epoch(command.commit.pg)?;
        if let Some(request) = &command.request {
            self.remember_vsr_request(
                command.commit.pg,
                VsrClientRequestId::new(request.client.0.clone(), request.request.0),
                command.commit.index,
            )?;
        }
        let result = match &command.request {
            Some(request) => self
                .router
                .append_existing_request(command.commit.clone(), request.clone())
                .map(|_| ()),
            None => self
                .router
                .append_existing_commit(command.commit.clone())
                .map(|_| ()),
        };
        if let Err(err) = result {
            self.application_failure = Some(err.message.clone());
            return Err(err);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn complete_protocol_codec_covers_operations_optional_requests_and_invalid_encodings() {
        let operations = vec![
            FabricOperation::CatalogMutation {
                namespace: "tenant/db".into(),
                item: "table".into(),
            },
            FabricOperation::TransactionCommit {
                transaction: [7; 16],
            },
            FabricOperation::RelationalCatalog {
                table: "users".into(),
                item: "index".into(),
            },
            FabricOperation::RelationalMutation {
                table: "users".into(),
                key: vec![0, 255],
            },
            FabricOperation::KvMutation { key: vec![0, 255] },
            FabricOperation::KvBatch {
                keys: vec![vec![], vec![0, 255]],
            },
            FabricOperation::ObjectBucket {
                bucket: "media".into(),
            },
            FabricOperation::ObjectMultipartStart {
                bucket: "media".into(),
                key: "key".into(),
                upload_id: 9,
            },
            FabricOperation::ObjectMultipartPart {
                bucket: "media".into(),
                key: "key".into(),
                upload_id: 9,
                part_number: 2,
            },
            FabricOperation::ObjectHead {
                bucket: "media".into(),
                key: "key".into(),
            },
            FabricOperation::ObjectManifest {
                bucket: "media".into(),
                key: "key".into(),
            },
            FabricOperation::BlobPin { digest: [8; 32] },
        ];
        for operation in operations {
            for request in [None, Some(ClientRequestId::new("metadata", 5))] {
                let command = ReplicatedProtocolCommand {
                    commit: FabricCommit {
                        pg: PlacementGroupId(1),
                        index: LogIndex(3),
                        version: CommitVersion(7),
                        mode: ConsistencyMode::Strict,
                        operation_id: operation.operation_id(),
                        operation: operation.clone(),
                    },
                    request,
                };
                let bytes = command.encode().unwrap();
                assert_eq!(ReplicatedProtocolCommand::decode(&bytes).unwrap(), command);
                for end in 0..bytes.len() {
                    assert!(ReplicatedProtocolCommand::decode(&bytes[..end]).is_err());
                }
                let mut trailing = bytes.clone();
                trailing.push(0);
                assert!(ReplicatedProtocolCommand::decode(&trailing).is_err());
                let mut invalid = command.clone();
                invalid.commit.operation_id = OperationId(0);
                assert!(invalid.encode().is_err());
                invalid = command.clone();
                invalid.commit.version = CommitVersion(0);
                assert!(invalid.encode().is_err());
            }
        }
    }
}
