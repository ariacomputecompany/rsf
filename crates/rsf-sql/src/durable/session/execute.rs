use super::*;

impl DurableSqlEngine {
    /// The caller authenticates owner and confirms read freshness, as for the
    /// autocommit API. Every mutating session transition is quorum replicated.
    pub fn execute_session(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        owner: &str,
        context: &SqlSessionContext,
        statement: &str,
        client: impl Into<String>,
    ) -> RsfResult<DurableSqlExecution> {
        self.ensure_healthy()?;
        if self.namespace.is_none() {
            return Err(invalid(
                "durable SQL sessions require a namespace-bound replicated store",
            ));
        }
        let request = SessionRequest {
            owner: owner.into(),
            context: context.clone(),
            client: client.into(),
            statement: statement.into(),
            pg: pg.0,
        };
        request.validate()?;
        if let Some(pending) = &self.pending
            && pending.client == request.client
        {
            pending.matches_session(&request)?;
        }
        self.reconcile(router)?;
        if let Some(receipt) = self.receipts.get(&request.client) {
            receipt.request.matches_session(&request)?;
            self.confirm_retry(router, &receipt.request)?;
            return self.execute_receipt(receipt, true);
        }
        if matches!(parse_statement(statement)?, ParsedStatement::Select { .. }) {
            let request = dependencies::read_request(self, request)?;
            if let Some(receipt) = self.receipts.get(&request.client) {
                receipt.request.matches_session(&request)?;
                self.confirm_retry(router, &receipt.request)?;
                return self.execute_receipt(receipt, true);
            }
            return self.execute_session_transition(router, pg, request);
        }
        self.execute_session_transition(router, pg, request)
    }

    fn execute_session_transition(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        request: SessionRequest,
    ) -> RsfResult<DurableSqlExecution> {
        let proposal = propose(self, request.clone(), router.latest_version(), true, true)?;
        let pending = PendingRequest {
            key_format: self.engine.key_format(),
            client: request.client,
            statement: request.statement,
            pg,
            session: Some(Box::new(proposal)),
        };
        let (candidate, command) = self.prepare_replicated(router, &pending)?;
        if let Err(error) = pending.persist(&self.root) {
            self.persistence_failed = true;
            return Err(error);
        }
        self.pending = Some(pending);
        self.submit_planned(router, candidate, command)
    }
}
