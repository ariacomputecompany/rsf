//! Complete non-application operations share the same ordered replication log.
use super::*;
use application::codec::{Reader, encode_operation, field};

#[derive(Clone, Debug)]
pub(super) enum PendingCommand {
    Application(ReplicatedFabricCommand),
    Protocol(ReplicatedProtocolCommand),
}

const MAGIC: &[u8; 5] = b"RSFG1";
const DEPENDENCY_MAGIC: &[u8; 5] = b"RSFI1";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolCommandPurpose {
    Operation,
    ImmutableDependency,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicatedProtocolCommand {
    pub purpose: ProtocolCommandPurpose,
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
        if self.purpose == ProtocolCommandPurpose::ImmutableDependency
            && (!matches!(self.commit.operation, FabricOperation::BlobPin { .. })
                || self.commit.mode != ConsistencyMode::AvailabilityFirst
                || self.request.is_none())
        {
            return Err(invalid(
                "immutable dependency must be an idempotent immutable pin",
            ));
        }
        let record = match &self.request {
            Some(request) => {
                ProtocolCommitRecord::from_client_request(&self.commit, request.clone())
            }
            None => ProtocolCommitRecord::from_commit(&self.commit),
        };
        let mut bytes = match self.purpose {
            ProtocolCommandPurpose::Operation => MAGIC,
            ProtocolCommandPurpose::ImmutableDependency => DEPENDENCY_MAGIC,
        }
        .to_vec();
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
        let purpose = match reader.take(5)? {
            value if value == MAGIC => ProtocolCommandPurpose::Operation,
            value if value == DEPENDENCY_MAGIC => ProtocolCommandPurpose::ImmutableDependency,
            _ => return Err(corrupt("unknown replicated protocol command format")),
        };
        let record = ProtocolCommitRecord::decode(reader.field()?)?;
        let operation = reader.operation()?;
        if reader.position != bytes.len() {
            return Err(corrupt("protocol command has trailing bytes"));
        }
        let certificate = record.certificate;
        let command = Self {
            purpose,
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
    /// Data-bearing surface operations also require a surface-owned payload.
    /// Their protocol receipt alone cannot restore a row, value, or object.
    pub(super) fn requires_surface_payload(&self) -> bool {
        self.purpose == ProtocolCommandPurpose::Operation
            && !matches!(
                self.commit.operation,
                FabricOperation::CatalogMutation { .. } | FabricOperation::TransactionCommit { .. }
            )
    }
    pub(super) fn is_payload(bytes: &[u8]) -> bool {
        bytes.starts_with(MAGIC) || bytes.starts_with(DEPENDENCY_MAGIC)
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
    pub(super) fn validate_dependency_request(
        &self,
        pg: PlacementGroupId,
        request: &ClientRequestId,
        digest: [u8; 32],
    ) -> RsfResult<()> {
        let operation = FabricOperation::BlobPin { digest };
        self.validate_request(pg, request, ConsistencyMode::AvailabilityFirst, &operation)?;
        if let Some(command) = self.protocol_request(request) {
            if command.purpose != ProtocolCommandPurpose::ImmutableDependency
                || command.commit.pg != pg
                || command.commit.mode != ConsistencyMode::AvailabilityFirst
                || command.commit.operation != operation
            {
                return Err(invalid(
                    "request identity belongs to a different immutable dependency",
                ));
            }
        } else if self.router.request_certificate(request).is_some() {
            return Err(invalid("legacy pin purpose requires explicit migration"));
        }
        Ok(())
    }

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
        self.require_vsr_commit(&command.commit, command.request.clone(), command.purpose)?;
        self.append_protocol_receipt(command)
    }

    pub(super) fn append_protocol_receipt(
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
                "protocol receipt would change its original version or index",
            ));
        }
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
        self.pending_commands.remove(&command.commit.version);
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
                    purpose: ProtocolCommandPurpose::Operation,
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

#[cfg(test)]
mod dependency_tests {
    use super::*;
    #[test]
    fn immutable_dependency_tag_requires_an_idempotent_immutable_pin_and_survives_replay() {
        let operation = FabricOperation::BlobPin { digest: [7; 32] };
        let command = ReplicatedProtocolCommand {
            purpose: ProtocolCommandPurpose::ImmutableDependency,
            commit: FabricCommit {
                pg: PlacementGroupId(1),
                index: LogIndex(1),
                version: CommitVersion(1),
                mode: ConsistencyMode::AvailabilityFirst,
                operation_id: operation.operation_id(),
                operation,
            },
            request: Some(ClientRequestId::new("internal-pin", 1)),
        };
        let bytes = command.encode().unwrap();
        assert_eq!(&bytes[..5], b"RSFI1");
        assert_eq!(ReplicatedProtocolCommand::decode(&bytes).unwrap(), command);
        assert!(!command.requires_surface_payload());
        for end in 0..bytes.len() {
            assert!(ReplicatedProtocolCommand::decode(&bytes[..end]).is_err());
        }
        let mut ordinary = bytes.clone();
        ordinary[..5].copy_from_slice(b"RSFG1");
        assert!(
            ReplicatedProtocolCommand::decode(&ordinary)
                .unwrap()
                .requires_surface_payload()
        );
        let mut invalid = command.clone();
        invalid.request = None;
        assert!(invalid.encode().is_err());
        invalid = command.clone();
        invalid.commit.mode = ConsistencyMode::Strict;
        assert!(invalid.encode().is_err());
        invalid = command;
        invalid.commit.operation = FabricOperation::CatalogMutation {
            namespace: "app".into(),
            item: "table".into(),
        };
        invalid.commit.operation_id = invalid.commit.operation.operation_id();
        assert!(invalid.encode().is_err());
    }
}
