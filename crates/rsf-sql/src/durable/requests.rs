use rsf_core::{CommitConsistency, CommitEvidence, LogIndex, OperationId};
use rsf_kv::ReadSnapshot;

use super::*;

pub(super) const INTENT_FILE: &str = "sql.intent";
const INTENT_TMP_FILE: &str = "sql.intent.tmp";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingRequest {
    pub key_format: RelationalKeyFormat,
    pub client: String,
    pub statement: String,
    pub pg: PlacementGroupId,
    pub session: Option<Box<session::SessionProposal>>,
}

impl PendingRequest {
    pub fn persist(&self, root: &Path) -> RsfResult<()> {
        let header = if self.key_format == RelationalKeyFormat::LegacyDelimitedV1 {
            "RSF_SQL_INTENT_V1"
        } else {
            "RSF_SQL_INTENT_V2"
        };
        let mut encoded = format!("{header}\n{}\n", self.encode()?);
        append_checksum(&mut encoded);
        atomic_write(root, INTENT_FILE, INTENT_TMP_FILE, &encoded)
    }

    pub fn read(root: &Path) -> RsfResult<Option<Self>> {
        let path = root.join(INTENT_FILE);
        let encoded = match fs::read_to_string(&path) {
            Ok(encoded) => encoded,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(corruption(format!(
                    "cannot read SQL intent {}: {err}",
                    path.display()
                )));
            }
        };
        let contents = verify_checksum(&encoded)?;
        let lines = contents.lines().collect::<Vec<_>>();
        match lines.as_slice() {
            [
                header @ ("RSF_SQL_INTENT_V1" | "RSF_SQL_INTENT_V2"),
                request,
            ] => {
                let pending = Self::decode(&request.split('\t').collect::<Vec<_>>())?;
                if (*header == "RSF_SQL_INTENT_V1")
                    != (pending.key_format == RelationalKeyFormat::LegacyDelimitedV1)
                {
                    return Err(corruption("SQL intent header differs from key format"));
                }
                Ok(Some(pending))
            }
            _ => Err(corruption("invalid SQL intent format")),
        }
    }

    pub fn encode(&self) -> RsfResult<String> {
        let mut text = format!(
            "{}\t{}\t{}",
            hex_bytes(self.client.as_bytes()),
            hex_bytes(self.statement.as_bytes()),
            self.pg.0
        );
        if self.key_format == RelationalKeyFormat::LengthPrefixedV2 {
            text.insert_str(0, "KEY2\t");
        }
        if let Some(session) = &self.session {
            text.push('\t');
            text.push_str(&hex_bytes(&session.encode()?));
        }
        Ok(text)
    }

    pub fn decode(fields: &[&str]) -> RsfResult<Self> {
        let (key_format, fields) = if let ["KEY2", rest @ ..] = fields {
            (RelationalKeyFormat::LengthPrefixedV2, rest)
        } else {
            (RelationalKeyFormat::LegacyDelimitedV1, fields)
        };
        let (client, statement, pg, session) = match fields {
            [client, statement, pg] => (*client, *statement, *pg, None),
            [client, statement, pg, proposal] => (
                *client,
                *statement,
                *pg,
                Some(Box::new(session::SessionProposal::decode(&hex_decode(
                    proposal,
                )?)?)),
            ),
            _ => return Err(corruption("malformed SQL request")),
        };
        let client = decode_string(client, "SQL request client")?;
        if client.trim().is_empty() {
            return Err(corruption("SQL request client is empty"));
        }
        let statement = decode_string(statement, "SQL request statement")?;
        let pg = PlacementGroupId(number(pg)?);
        if let Some(proposal) = &session {
            if proposal.request.client != client
                || proposal.request.statement != statement
                || proposal.request.pg != pg.0
            {
                return Err(corruption(
                    "SQL session request differs from its transition",
                ));
            }
        } else {
            reject_transaction_control(&statement).map_err(|err| corruption(err.message))?;
            if matches!(parse_statement(&statement)?, ParsedStatement::Select { .. }) {
                return Err(corruption("read-only SQL request must not be journaled"));
            }
        }
        Ok(Self {
            key_format,
            client,
            statement,
            pg,
            session,
        })
    }
    pub fn matches_request(&self, other: &Self) -> RsfResult<()> {
        if self != other {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SQL idempotency key was used for a different complete request",
            ));
        }
        Ok(())
    }
    pub fn matches_session(&self, request: &session::SessionRequest) -> RsfResult<()> {
        if self
            .session
            .as_ref()
            .is_none_or(|proposal| proposal.request != *request)
        {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SQL idempotency key was used for a different session request",
            ));
        }
        Ok(())
    }

    pub fn matches(&self, pg: PlacementGroupId, statement: &str) -> RsfResult<()> {
        if self.session.is_some() || self.pg != pg || self.statement != statement {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SQL idempotency key was already used for a different request",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(super) struct RequestReceipt {
    pub request: PendingRequest,
    pub evidence: CommitEvidence,
}

impl RequestReceipt {
    pub fn encode(&self) -> RsfResult<String> {
        Ok(format!(
            "request\t{}\t{}\t{}\t{}\n",
            self.request.encode()?,
            self.evidence.log_index.0,
            self.evidence.version.0,
            self.evidence.operation_id.0
        ))
    }

    pub fn decode(fields: &[&str]) -> RsfResult<Self> {
        if !(6..=8).contains(&fields.len()) {
            return Err(corruption("malformed SQL request receipt"));
        }
        let split = fields.len() - 3;
        let request = PendingRequest::decode(&fields[..split])?;
        let [index, version, operation] = &fields[split..] else {
            unreachable!("three evidence fields")
        };
        let evidence = CommitEvidence {
            placement_group: request.pg,
            log_index: LogIndex(number(index)?),
            version: CommitVersion(number(version)?),
            operation_id: OperationId(number(operation)?),
            consistency: CommitConsistency::Strict,
        };
        if evidence.log_index.0 == 0 || evidence.version.is_genesis() {
            return Err(corruption("SQL receipt has no committed evidence"));
        }
        Ok(Self { request, evidence })
    }

    // Receipts share the atomic checkpoint with retained MVCC versions. Returning
    // the row at the receipt's version preserves the original result after updates.
    pub fn outcome(&self, engine: &SqlEngine) -> RsfResult<SqlOutcome> {
        let evidence = self.evidence.clone();
        if let Some(proposal) = &self.request.session {
            return Ok(proposal.result.outcome(&evidence));
        }
        let row_at_commit = |table: &str, primary_key: &str| {
            let key = engine.row_key(table, primary_key);
            let value = engine
                .kv
                .get_at(
                    &key,
                    ReadSnapshot {
                        version: evidence.version,
                    },
                )
                .filter(|value| value.version == evidence.version)
                .ok_or_else(|| corruption("SQL receipt is missing its committed row version"))?;
            engine.row_from_physical_bytes(
                engine.table_record(table)?,
                primary_key,
                &value.value,
                value.version,
            )
        };
        match parse_statement(&self.request.statement)? {
            ParsedStatement::CreateTable { table, .. } => {
                engine.table_record(&table)?;
                Ok(SqlOutcome::TableCreated {
                    table,
                    version: evidence.version,
                    evidence,
                })
            }
            ParsedStatement::CreateIndex { table, column } => {
                if !engine
                    .table_record(&table)?
                    .secondary_indexes
                    .contains(&column)
                {
                    return Err(corruption("SQL index receipt has no recovered index"));
                }
                Ok(SqlOutcome::IndexCreated {
                    table,
                    column,
                    version: evidence.version,
                    evidence,
                })
            }
            ParsedStatement::Insert { table, columns } => {
                let key = engine.primary_key_value(&table, &columns)?;
                Ok(SqlOutcome::Inserted {
                    row: row_at_commit(&table, &key)?,
                    evidence: SqlMutationEvidence::Committed(evidence),
                })
            }
            ParsedStatement::Update {
                table, predicate, ..
            } => Ok(SqlOutcome::Updated {
                row: row_at_commit(&table, &predicate.value)?,
                evidence: SqlMutationEvidence::Committed(evidence),
            }),
            ParsedStatement::Delete { table, predicate } => {
                let key = engine.row_key(&table, &predicate.value);
                let versions = engine.kv.scan_prefix_versions(&key);
                if !versions.iter().any(|(candidate, values)| {
                    candidate == &key
                        && values
                            .iter()
                            .any(|value| value.version == evidence.version && value.deleted)
                }) {
                    return Err(corruption("SQL delete receipt is missing its tombstone"));
                }
                Ok(SqlOutcome::Deleted {
                    table,
                    primary_key: predicate.value,
                    evidence: SqlMutationEvidence::Committed(evidence),
                })
            }
            _ => Err(corruption(
                "SQL receipt must describe an autocommit mutation",
            )),
        }
    }
}

fn number(value: &str) -> RsfResult<u64> {
    value
        .parse()
        .map_err(|_| corruption("SQL request contains an invalid integer"))
}

fn corruption(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}
