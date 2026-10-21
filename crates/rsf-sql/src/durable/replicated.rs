//! Deterministic autocommit statements on the complete-command VSR spine. A
//! checkpoint publishes namespace, relational state and command receipts together.
use super::*;
use rsf_protocol::{
    ApplicationPayload, ApplicationSurface, ConsistencyMode, FabricOperation,
    ReplicatedFabricCommand,
};

mod plan;
const MAGIC: &[u8] = b"RSFSC1";
fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}
fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}
pub(super) fn validate_namespace(namespace: &str) -> RsfResult<()> {
    if namespace.is_empty() || namespace.len() > 4096 || namespace.chars().any(char::is_control) {
        return Err(invalid("invalid SQL replication namespace"));
    }
    Ok(())
}
fn application(namespace: &str, request: &PendingRequest) -> ApplicationPayload {
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(request.encode().as_bytes());
    ApplicationPayload {
        namespace: namespace.into(),
        surface: ApplicationSurface::Sql,
        bytes,
    }
}
fn decode_request(command: &ReplicatedFabricCommand, namespace: &str) -> RsfResult<PendingRequest> {
    command.encode()?;
    if command.application.namespace != namespace
        || command.application.surface != ApplicationSurface::Sql
        || command.commit.mode != ConsistencyMode::Strict
    {
        return Err(invalid(
            "SQL command targets a different namespace, surface or consistency mode",
        ));
    }
    let text = command
        .application
        .bytes
        .strip_prefix(MAGIC)
        .ok_or_else(|| corrupt("unknown SQL command format"))?;
    let text = std::str::from_utf8(text).map_err(|_| corrupt("SQL command is not UTF-8"))?;
    let request = PendingRequest::decode(&text.split('\t').collect::<Vec<_>>())?;
    if command.request != ClientRequestId::new(request.client.clone(), 0)
        || command.commit.pg != request.pg
    {
        return Err(corrupt(
            "SQL command request or placement differs from its body",
        ));
    }
    Ok(request)
}
pub(super) fn validate_checkpoint_commands(
    namespace: Option<&str>,
    commands: &BTreeMap<ClientRequestId, ReplicatedFabricCommand>,
    receipts: &BTreeMap<String, RequestReceipt>,
) -> RsfResult<()> {
    let Some(namespace) = namespace else {
        return if commands.is_empty() {
            Ok(())
        } else {
            Err(corrupt("SQL commands have no namespace binding"))
        };
    };
    validate_namespace(namespace)?;
    if commands.len() != receipts.len() {
        return Err(corrupt("bound SQL receipts require complete commands"));
    }
    let mut versions = std::collections::BTreeSet::new();
    for command in commands.values() {
        let request = decode_request(command, namespace)?;
        let receipt = receipts
            .get(&request.client)
            .ok_or_else(|| corrupt("SQL command has no receipt"))?;
        receipt.request.matches(request.pg, &request.statement)?;
        if receipt.evidence != crate::commit_evidence(command.certificate())
            || !versions.insert(command.commit.version)
        {
            return Err(corrupt(
                "SQL command evidence conflicts with receipt history",
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_recovered_operations(
    engine: &SqlEngine,
    commands: &BTreeMap<ClientRequestId, ReplicatedFabricCommand>,
) -> RsfResult<()> {
    for command in commands.values() {
        let request = decode_request(command, &command.application.namespace)?;
        if plan::operation(engine, &request.statement)? != command.commit.operation {
            return Err(corrupt(
                "SQL checkpoint command differs from recovered relational operation",
            ));
        }
    }
    Ok(())
}

impl DurableSqlEngine {
    pub fn open_replicated(root: impl AsRef<Path>, namespace: &str) -> RsfResult<Self> {
        validate_namespace(namespace)?;
        let mut sql = Self::open(root)?;
        if let Some(existing) = &sql.namespace {
            return if existing == namespace {
                Ok(sql)
            } else {
                Err(invalid("SQL store belongs to a different namespace"))
            };
        }
        if !sql.engine.recovered_tables_for_checkpoint().is_empty()
            || !sql.receipts.is_empty()
            || sql.pending.is_some()
            || !sql.engine.committed_kv_versions_for_checkpoint().is_empty()
        {
            return Err(invalid(
                "nonempty legacy SQL store requires explicit namespace migration",
            ));
        }
        sql.namespace = Some(namespace.into());
        sql.persist()?;
        Ok(sql)
    }
    fn ensure_healthy(&self) -> RsfResult<()> {
        if self.persistence_failed {
            Err(corrupt(
                "durable SQL state failed; reopen before further operations",
            ))
        } else {
            Ok(())
        }
    }
    fn highest_command(&self) -> CommitVersion {
        self.commands
            .values()
            .map(|command| command.commit.version)
            .max()
            .unwrap_or(CommitVersion::GENESIS)
    }
    /// Inspect the local materialized snapshot. This does not establish leader
    /// freshness; network callers must confirm their read consistency separately.
    pub fn read_snapshot(&mut self, statement: &str) -> RsfResult<DurableSqlExecution> {
        self.ensure_healthy()?;
        if self.pending.is_some() {
            return Err(corrupt("resolve the pending SQL statement before reading"));
        }
        let ParsedStatement::Select {
            table,
            projection,
            predicate,
            limit,
        } = parse_statement(statement)?
        else {
            return Err(invalid("SQL snapshot inspection requires SELECT"));
        };
        let rows = self
            .engine
            .execute_select(&table, projection, predicate, limit)?;
        Ok(execution(SqlOutcome::Selected { rows }, false))
    }

    /// Resolve the local pending statement before exposing SQL reads. Callers
    /// first reconcile and apply the router's complete committed command history.
    pub fn reconcile(&mut self, router: &mut impl FabricRouterPort) -> RsfResult<usize> {
        self.ensure_healthy()?;
        if self.namespace.is_some() && router.latest_version() < self.highest_command() {
            return Err(corrupt("SQL state is ahead of recovered protocol history"));
        }
        let Some(pending) = self.pending.clone() else {
            return Ok(0);
        };
        if self.namespace.is_some() {
            if self.receipts.contains_key(&pending.client) {
                self.confirm_retry(router, &pending)?;
                self.clear_pending()?;
            } else {
                let (candidate, command) = self.prepare_replicated(router, &pending)?;
                self.submit_planned(router, candidate, command)?;
            }
        } else {
            self.finish_pending(router, pending)?;
        }
        Ok(1)
    }

    pub(super) fn execute_replicated(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        statement: &str,
        client: String,
    ) -> RsfResult<DurableSqlExecution> {
        self.ensure_healthy()?;
        reject_transaction_control(statement)?;
        if client.trim().is_empty() {
            return Err(invalid("SQL request client must not be empty"));
        }
        if let Some(pending) = &self.pending
            && pending.client == client
        {
            pending.matches(pg, statement)?;
        }
        self.reconcile(router)?;
        let request = PendingRequest {
            client,
            statement: statement.into(),
            pg,
        };
        if self.receipts.contains_key(&request.client) {
            self.confirm_retry(router, &request)?;
            return Ok(execution(
                self.receipts[&request.client].outcome(&self.engine)?,
                true,
            ));
        }
        if matches!(parse_statement(statement)?, ParsedStatement::Select { .. }) {
            return self.read_snapshot(statement);
        }
        let (candidate, command) = self.prepare_replicated(router, &request)?;
        if let Err(err) = request.persist(&self.root) {
            self.persistence_failed = true;
            return Err(err);
        }
        self.pending = Some(request);
        self.submit_planned(router, candidate, command)
    }

    fn prepare_replicated(
        &self,
        router: &mut impl FabricRouterPort,
        pending: &PendingRequest,
    ) -> RsfResult<(SqlEngine, ReplicatedFabricCommand)> {
        let namespace = self
            .namespace
            .as_deref()
            .ok_or_else(|| invalid("SQL planning requires a bound namespace"))?;
        let operation = plan::operation(&self.engine, &pending.statement)?;
        let application = application(namespace, pending);
        let request = ClientRequestId::new(pending.client.clone(), 0);
        let commit = router.preview_application_request(
            pending.pg,
            &request,
            ConsistencyMode::Strict,
            &operation,
            &application,
        )?;
        let command = ReplicatedFabricCommand {
            request,
            commit,
            application,
        };
        decode_request(&command, namespace)?;
        if command.commit.operation != operation {
            return Err(corrupt("SQL preview differs from its operation"));
        }
        if command.commit.version <= self.highest_command() {
            return Err(corrupt("SQL command reorders applied history"));
        }
        let candidate = plan::prepare(
            &self.engine,
            &pending.statement,
            pending.pg,
            command.commit.version,
        )?;
        Ok((candidate, command))
    }

    fn submit_planned(
        &mut self,
        router: &mut impl FabricRouterPort,
        candidate: SqlEngine,
        command: ReplicatedFabricCommand,
    ) -> RsfResult<DurableSqlExecution> {
        let result = router.commit_application_request(
            command.commit.pg,
            command.request.clone(),
            command.commit.mode,
            command.commit.operation.clone(),
            command.application.clone(),
        );
        let receipt = match result {
            Ok(receipt) => receipt,
            Err(err) => {
                self.persistence_failed = true;
                return Err(err);
            }
        };
        if receipt.certificate != command.certificate() {
            self.persistence_failed = true;
            return Err(corrupt(
                "SQL planning metadata changed before commit; recover authoritative history",
            ));
        }
        self.publish_command(candidate, command, receipt.replayed)
    }

    fn confirm_retry(
        &self,
        router: &mut impl FabricRouterPort,
        request: &PendingRequest,
    ) -> RsfResult<()> {
        let receipt = self
            .receipts
            .get(&request.client)
            .ok_or_else(|| corrupt("SQL retry has no receipt"))?;
        receipt.request.matches(request.pg, &request.statement)?;
        let id = ClientRequestId::new(request.client.clone(), 0);
        let command = self
            .commands
            .get(&id)
            .ok_or_else(|| corrupt("SQL retry has no complete command"))?;
        if router.latest_version() < self.highest_command()
            || router.request_certificate(&id) != Some(command.certificate())
        {
            return Err(corrupt(
                "SQL retry requires matching recovered protocol history",
            ));
        }
        let retry = router.commit_application_request(
            command.commit.pg,
            id,
            command.commit.mode,
            command.commit.operation.clone(),
            command.application.clone(),
        )?;
        if retry.certificate != command.certificate() {
            return Err(corrupt("SQL retry returned different commit evidence"));
        }
        Ok(())
    }

    /// Apply only authenticated, quorum-validated commands. The supported grammar
    /// is deterministic; each command must reproduce its exact relational operation.
    pub fn apply_application_command(
        &mut self,
        command: &ReplicatedFabricCommand,
    ) -> RsfResult<DurableSqlExecution> {
        self.ensure_healthy()?;
        let namespace = self
            .namespace
            .as_deref()
            .ok_or_else(|| invalid("SQL replay requires a bound namespace"))?;
        let request = decode_request(command, namespace)?;
        if let Some(original) = self.commands.get(&command.request) {
            if original != command {
                return Err(corrupt(
                    "SQL replay differs from its original complete command",
                ));
            }
            if let Some(pending) = &self.pending
                && pending.client == request.client
            {
                pending.matches(request.pg, &request.statement)?;
                self.clear_pending()?;
            }
            return Ok(execution(
                self.receipts[&request.client].outcome(&self.engine)?,
                true,
            ));
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending != &request)
        {
            return Err(corrupt(
                "SQL replay would overtake a different pending statement",
            ));
        }
        if command.commit.version <= self.highest_command()
            || self.receipts.contains_key(&request.client)
            || plan::operation(&self.engine, &request.statement)? != command.commit.operation
        {
            return Err(corrupt(
                "SQL replay operation conflicts with applied history",
            ));
        }
        let candidate = plan::prepare(
            &self.engine,
            &request.statement,
            request.pg,
            command.commit.version,
        )?;
        self.publish_command(candidate, command.clone(), false)
    }

    fn publish_command(
        &mut self,
        candidate: SqlEngine,
        command: ReplicatedFabricCommand,
        replayed: bool,
    ) -> RsfResult<DurableSqlExecution> {
        let namespace = self
            .namespace
            .as_deref()
            .ok_or_else(|| invalid("SQL publication requires a bound namespace"))?;
        let request = decode_request(&command, namespace)?;
        let receipt = RequestReceipt {
            request: request.clone(),
            evidence: crate::commit_evidence(command.certificate()),
        };
        let outcome = match receipt.outcome(&candidate) {
            Ok(outcome) => outcome,
            Err(err) => {
                self.persistence_failed = true;
                return Err(err);
            }
        };
        self.engine = candidate;
        self.commands.insert(command.request.clone(), command);
        self.receipts.insert(request.client, receipt);
        if let Err(err) = self.persist() {
            self.persistence_failed = true;
            return Err(err);
        }
        if self.pending.is_some() {
            self.clear_pending()?;
        }
        Ok(execution(outcome, replayed))
    }
}

#[cfg(test)]
mod tests;
