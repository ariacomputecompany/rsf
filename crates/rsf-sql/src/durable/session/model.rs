use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::durable) struct SessionRequest {
    pub owner: String,
    pub context: SqlSessionContext,
    pub client: String,
    pub statement: String,
    pub pg: u64,
}
impl SessionRequest {
    pub fn key(&self) -> String {
        key(&self.owner, &self.context.name)
    }
    pub fn validate(&self) -> RsfResult<()> {
        validate_name(&self.owner, "session owner", 256)?;
        validate_name(&self.client, "request identity", 4096)?;
        if self.statement.is_empty() || self.statement.len() > 64 * 1024 {
            return Err(invalid("invalid SQL session statement size"));
        }
        self.context.validate()?;
        parse_statement(&self.statement)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::durable) struct SessionState {
    pub token: String,
    pub snapshot: u64,
    pub schema: [u8; 32],
    pub pg: u64,
    pub statements: Vec<String>,
    pub savepoints: Vec<(String, usize)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<std::collections::BTreeSet<super::dependencies::Dependency>>,
}
impl SessionState {
    pub fn validate(&self) -> RsfResult<()> {
        validate_token(&self.token)?;
        if let Some(dependencies) = &self.dependencies {
            if dependencies.len() > MAX_SQL_SESSION_DEPENDENCIES {
                return Err(invalid("SQL session read dependency limit reached"));
            }
            for dependency in dependencies {
                dependency.validate()?;
            }
        }
        if self.statements.len() > MAX_SQL_SESSION_STATEMENTS
            || self.savepoints.len() > MAX_SQL_SAVEPOINTS
            || encode(self)?.len() > MAX_SQL_SESSION_BYTES
        {
            return Err(invalid(
                "SQL session exceeds its statement, savepoint or byte limit",
            ));
        }
        for statement in &self.statements {
            if !matches!(
                parse_statement(statement)?,
                ParsedStatement::CreateTable { .. }
                    | ParsedStatement::CreateIndex { .. }
                    | ParsedStatement::Insert { .. }
                    | ParsedStatement::Update { .. }
                    | ParsedStatement::Delete { .. }
            ) {
                return Err(corrupt("non-mutation in staged SQL session history"));
            }
        }
        let mut previous = 0;
        for (name, position) in &self.savepoints {
            crate::validate_savepoint_name(name)?;
            if *position > self.statements.len() || *position < previous {
                return Err(corrupt("invalid SQL session savepoint position"));
            }
            previous = *position;
        }
        Ok(())
    }
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        self.validate()?;
        encode(self)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() > MAX_SQL_SESSION_BYTES {
            return Err(corrupt("SQL session state exceeds limit"));
        }
        let state: Self = decode(bytes)?;
        state.validate()?;
        Ok(state)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::durable) struct SessionRow {
    pub table: String,
    pub key: String,
    pub columns: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::durable) struct RowEffect {
    pub table: String,
    pub key: String,
    pub columns: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(in crate::durable) enum SessionResult {
    Selected {
        token: String,
        snapshot: u64,
        rows: Vec<SessionRow>,
    },
    Began {
        token: String,
        snapshot: u64,
    },
    Schema {
        token: String,
        snapshot: u64,
        table: String,
        item: String,
    },
    Inserted {
        token: String,
        snapshot: u64,
        row: SessionRow,
    },
    Updated {
        token: String,
        snapshot: u64,
        row: SessionRow,
    },
    Deleted {
        token: String,
        snapshot: u64,
        table: String,
        key: String,
    },
    Savepoint {
        token: String,
        name: String,
        staged: usize,
    },
    RollbackTo {
        token: String,
        name: String,
        staged: usize,
    },
    Committed {
        token: String,
        mutations: usize,
    },
    RolledBack {
        token: String,
        discarded: usize,
    },
}
impl SessionResult {
    pub fn outcome(&self, evidence: &rsf_core::CommitEvidence) -> SqlOutcome {
        let staged = |token: &String, snapshot: &u64| crate::SqlMutationEvidence::Staged {
            session_id: token.clone(),
            snapshot: CommitVersion(*snapshot),
        };
        let row = |row: &SessionRow, snapshot: &u64| crate::Row {
            table: row.table.clone(),
            primary_key: row.key.clone(),
            columns: row.columns.clone(),
            version: CommitVersion(*snapshot),
        };
        match self {
            Self::Selected { rows, .. } => SqlOutcome::Selected {
                rows: rows.iter().map(|value| row(value, &0)).collect(),
            },
            Self::Began { token, snapshot } => SqlOutcome::TransactionBegan {
                session_id: token.clone(),
                snapshot: CommitVersion(*snapshot),
            },
            Self::Schema {
                token,
                snapshot,
                table,
                item,
            } => SqlOutcome::SchemaStaged {
                session_id: token.clone(),
                snapshot: CommitVersion(*snapshot),
                table: table.clone(),
                item: item.clone(),
            },
            Self::Inserted {
                token,
                snapshot,
                row: value,
            } => SqlOutcome::Inserted {
                row: row(value, snapshot),
                evidence: staged(token, snapshot),
            },
            Self::Updated {
                token,
                snapshot,
                row: value,
            } => SqlOutcome::Updated {
                row: row(value, snapshot),
                evidence: staged(token, snapshot),
            },
            Self::Deleted {
                token,
                snapshot,
                table,
                key,
            } => SqlOutcome::Deleted {
                table: table.clone(),
                primary_key: key.clone(),
                evidence: staged(token, snapshot),
            },
            Self::Savepoint {
                token,
                name,
                staged,
            } => SqlOutcome::SavepointCreated {
                session_id: token.clone(),
                name: name.clone(),
                staged: *staged,
            },
            Self::RollbackTo {
                token,
                name,
                staged,
            } => SqlOutcome::SavepointRolledBack {
                session_id: token.clone(),
                name: name.clone(),
                staged: *staged,
            },
            Self::Committed { token, mutations } => SqlOutcome::TransactionCommitted {
                session_id: token.clone(),
                evidence: evidence.clone(),
                mutations: *mutations,
            },
            Self::RolledBack { token, discarded } => SqlOutcome::TransactionRolledBack {
                session_id: token.clone(),
                discarded: *discarded,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::durable) struct SessionProposal {
    pub request: SessionRequest,
    pub before: Option<SessionState>,
    pub after: Option<SessionState>,
    pub result: SessionResult,
    pub effects: Vec<RowEffect>,
    // Omission preserves canonical RSFSP1 bytes and their original certificates.
    #[serde(default, skip_serializing_if = "snapshot_isolation")]
    pub serializable: bool,
    #[serde(default, skip_serializing_if = "snapshot_isolation")]
    pub tracked: bool,
}
fn snapshot_isolation(serializable: &bool) -> bool {
    !serializable
}
impl SessionProposal {
    pub fn validate(&self) -> RsfResult<()> {
        self.request.validate()?;
        if !self.tracked
            && matches!(
                parse_statement(&self.request.statement)?,
                ParsedStatement::Select { .. }
            )
        {
            return Err(corrupt(
                "read-only SQL session command cannot be replicated",
            ));
        }
        if self.tracked && !self.serializable {
            return Err(corrupt(
                "tracked SQL session requires serializable isolation",
            ));
        }
        if !self.tracked
            && [self.before.as_ref(), self.after.as_ref()]
                .into_iter()
                .flatten()
                .any(|state| state.dependencies.is_some())
        {
            return Err(corrupt(
                "legacy SQL command cannot carry tracked dependency state",
            ));
        }
        if let Some(before) = &self.before {
            before.validate()?;
        }
        if let Some(after) = &self.after {
            after.validate()?;
        }
        super::validation::validate(self)?;
        if !matches!(self.result, SessionResult::Committed { .. }) && !self.effects.is_empty() {
            return Err(corrupt("uncommitted session transition has row effects"));
        }
        if encode(self)?.len() > MAX_PROPOSAL_BYTES {
            return Err(invalid("SQL session transition exceeds payload limit"));
        }
        Ok(())
    }
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        self.validate()?;
        let mut bytes = if self.tracked {
            TRACKED_PROPOSAL_MAGIC
        } else if self.serializable {
            SERIALIZABLE_PROPOSAL_MAGIC
        } else {
            PROPOSAL_MAGIC
        }
        .to_vec();
        bytes.extend(encode(self)?);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() > MAX_PROPOSAL_BYTES + PROPOSAL_MAGIC.len() {
            return Err(corrupt("SQL session transition exceeds limit"));
        }
        let (body, serializable, tracked) =
            if let Some(body) = bytes.strip_prefix(TRACKED_PROPOSAL_MAGIC) {
                (body, true, true)
            } else if let Some(body) = bytes.strip_prefix(SERIALIZABLE_PROPOSAL_MAGIC) {
                (body, true, false)
            } else if let Some(body) = bytes.strip_prefix(PROPOSAL_MAGIC) {
                (body, false, false)
            } else {
                return Err(corrupt("invalid SQL session transition format"));
            };
        let value: Self = decode(body)?;
        if value.serializable != serializable || value.tracked != tracked {
            return Err(corrupt(
                "SQL session isolation differs from transition format",
            ));
        }
        value.validate()?;
        Ok(value)
    }
    pub fn operation(&self) -> RsfResult<rsf_protocol::FabricOperation> {
        Ok(rsf_protocol::FabricOperation::SqlSession {
            owner: self.request.owner.clone(),
            session: self.request.context.name.clone(),
            digest: Digest::blake3_bytes(&self.encode()?).bytes,
        })
    }
}
