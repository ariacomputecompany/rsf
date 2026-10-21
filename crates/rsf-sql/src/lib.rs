#![forbid(unsafe_code)]

mod durable;
mod pages;

use std::collections::{BTreeMap, BTreeSet};

use rsf_buffer::{BufferDecision, BufferPool};
use rsf_catalog::CatalogEngine;
use rsf_constraints::{
    ConstraintDecision, ConstraintRule, ConstraintState, ConstraintValidator, ConstraintValue,
    RowMutation,
};
use rsf_core::{
    CommitConsistency, CommitEvidence, CommitVersion, ErrorCode, PlacementGroupId, RsfError,
    RsfResult,
    catalog::{ColumnDescriptor, ColumnType, NamespaceName, TableDescriptor, TableName},
    transaction::TransactionId,
};
use rsf_executor::{ExecutionReport, PhysicalExecutor};
use rsf_index::{IndexDescriptor, IndexPlanner, IndexStore, RowImage, encode_index_key};
use rsf_kv::{KvEngine, KvVersionedValue, ReadSnapshot};
use rsf_page::{
    PageDecision, PageFrame, PageId, PageServer, RedoOperation, RedoRecord, ReplayReport,
};
use rsf_planner::{AccessPath, PhysicalPlan, PhysicalPlanner, PhysicalPlanningContext, TableStats};
use rsf_portal::{PortalDecision, SqlPortalSession};
use rsf_prepared::{PreparedParamType, PreparedValue};
use rsf_primary::{PrimaryDecision, PrimaryIndex, PrimaryKeyDescriptor, PrimaryRow};
use rsf_protocol::{
    ClientRequestId, CommitCertificate, ConsistencyMode, FabricOperation, FabricRouterPort,
};
use rsf_query::{
    Predicate, Projection, QueryExecutor, QueryPlan, QueryPlanner, QueryRow, QueryValue,
};
use rsf_rowcodec::{RowCodec, RowCodecPolicy, RowValue};
use rsf_stats::{
    ColumnSample, Freshness, StatisticsCatalog, TableEstimate, TableSample, ValueSample,
};
use rsf_tuple::{TupleDecision, TupleKey, TupleStore, TupleVersion};
use rsf_vacuum::{VacuumDecision, VacuumPlanner, VacuumPolicy};
use rsf_wal::{FlushRequest, WalDecision, WalGate, WalPolicy};

pub use durable::{DurableSqlEngine, DurableSqlExecution, DurableSqlRecovery};

const DEFAULT_PAGE_SIZE: u32 = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlTableLayout {
    OrderedKv,
    PageWal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlLayoutProfile {
    OrderedKv,
    PageWal,
    Hybrid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlLayoutDecision {
    pub table: String,
    pub profile: SqlLayoutProfile,
    pub layout: SqlTableLayout,
    pub page_size: u32,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Table {
    pub name: String,
    pub primary_key: String,
    pub columns: BTreeSet<String>,
    pub descriptor: TableDescriptor,
    pub primary_descriptor: PrimaryKeyDescriptor,
    pub constraints: Vec<ConstraintRule>,
    pub secondary_indexes: Vec<String>,
    pub layout_profile: SqlLayoutProfile,
    pub layout: SqlTableLayout,
    pub page_size: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Row {
    pub table: String,
    pub primary_key: String,
    pub columns: BTreeMap<String, String>,
    pub version: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlRecoveredTable {
    pub descriptor: TableDescriptor,
    pub profile: SqlLayoutProfile,
    pub constraints: Vec<ConstraintRule>,
    pub secondary_indexes: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlRecoveryReport {
    pub tables: usize,
    pub row_versions: usize,
    pub live_rows: usize,
    pub tombstones: usize,
    pub primary_index_entries: usize,
    pub secondary_index_entries: usize,
    pub unique_constraint_keys: usize,
    pub referenced_constraint_keys: usize,
    pub highest_version: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlMutationEvidence {
    Committed(CommitEvidence),
    Staged {
        session_id: String,
        snapshot: CommitVersion,
    },
}

impl SqlMutationEvidence {
    pub fn committed(&self) -> Option<&CommitEvidence> {
        match self {
            Self::Committed(evidence) => Some(evidence),
            Self::Staged { .. } => None,
        }
    }

    pub fn expect_committed(&self, context: &str) -> &CommitEvidence {
        self.committed()
            .unwrap_or_else(|| panic!("{context} did not produce committed SQL evidence"))
    }

    pub fn version(&self) -> CommitVersion {
        match self {
            Self::Committed(evidence) => evidence.version,
            Self::Staged { snapshot, .. } => *snapshot,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlOutcome {
    TableCreated {
        table: String,
        version: CommitVersion,
        evidence: CommitEvidence,
    },
    IndexCreated {
        table: String,
        column: String,
        version: CommitVersion,
        evidence: CommitEvidence,
    },
    Inserted {
        row: Row,
        evidence: SqlMutationEvidence,
    },
    Updated {
        row: Row,
        evidence: SqlMutationEvidence,
    },
    Deleted {
        table: String,
        primary_key: String,
        evidence: SqlMutationEvidence,
    },
    Selected {
        rows: Vec<Row>,
    },
    TransactionBegan {
        session_id: String,
        snapshot: CommitVersion,
    },
    SavepointCreated {
        session_id: String,
        name: String,
        staged: usize,
    },
    SavepointRolledBack {
        session_id: String,
        name: String,
        staged: usize,
    },
    TransactionCommitted {
        session_id: String,
        evidence: CommitEvidence,
        mutations: usize,
    },
    TransactionRolledBack {
        session_id: String,
        discarded: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlQueryReport {
    pub logical: QueryPlan,
    pub physical: PhysicalPlan,
    pub execution: ExecutionReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlPortalExecution {
    pub decision: PortalDecision,
    pub outcome: SqlOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlVacuumReport {
    pub table: String,
    pub chains: usize,
    pub pruned_versions: usize,
    pub retained_versions: usize,
    pub reclaimed_bytes: u64,
    pub rejections: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlPageWalReport {
    pub pages: usize,
    pub page_redo: usize,
    pub wal_entries: usize,
    pub durable_lsn: CommitVersion,
    pub checkpoint_lsn: CommitVersion,
    pub checkpoint_bytes: u64,
    pub checkpoint_live_rows: u64,
    pub checkpoint_tombstones: u64,
    pub buffer_resident: usize,
    pub ordered_kv_tables: usize,
    pub page_wal_tables: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlPageWalSnapshot {
    pub page_images: Vec<Vec<u8>>,
    pub redo_records: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SqlTransactionState {
    id: TransactionId,
    session_id: String,
    snapshot: CommitVersion,
    staged: BTreeMap<(String, String), SqlTransactionMutation>,
    savepoints: BTreeMap<String, BTreeMap<(String, String), SqlTransactionMutation>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SqlTransactionMutation {
    Set(BTreeMap<String, String>),
    Delete,
}

#[derive(Clone, Debug)]
pub struct SqlEngine {
    tables: BTreeMap<String, Table>,
    rows: BTreeMap<(String, String), Row>,
    primary: PrimaryIndex,
    tuples: TupleStore,
    constraints: ConstraintState,
    indexes: IndexStore,
    kv: KvEngine,
    pages: PageServer,
    wal: WalGate,
    buffer: BufferPool,
    table_pages: BTreeMap<String, BTreeSet<PageId>>,
    row_pages: BTreeMap<Vec<u8>, PageId>,
    page_wal_base_images: BTreeMap<PageId, Vec<u8>>,
    page_wal_redo_records: Vec<Vec<u8>>,
    layout_decisions: BTreeMap<String, SqlLayoutDecision>,
    next_page_lsn: u64,
    query_reports: Vec<SqlQueryReport>,
    portals: SqlPortalSession,
    stats: StatisticsCatalog,
    schema_version: CommitVersion,
    transaction: Option<SqlTransactionState>,
    next_transaction: u64,
    request_client: Option<String>,
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self {
            tables: BTreeMap::new(),
            rows: BTreeMap::new(),
            primary: PrimaryIndex::default(),
            tuples: TupleStore::default(),
            constraints: ConstraintState::default(),
            indexes: IndexStore::default(),
            kv: KvEngine::default(),
            pages: PageServer::default(),
            wal: WalGate::new(WalPolicy::new(1024 * 1024, true).expect("valid wal policy")),
            buffer: BufferPool::new(128).expect("valid buffer capacity"),
            table_pages: BTreeMap::new(),
            row_pages: BTreeMap::new(),
            page_wal_base_images: BTreeMap::new(),
            page_wal_redo_records: Vec::new(),
            layout_decisions: BTreeMap::new(),
            next_page_lsn: 1,
            query_reports: Vec::new(),
            portals: SqlPortalSession::default(),
            stats: StatisticsCatalog::default(),
            schema_version: CommitVersion::GENESIS,
            transaction: None,
            next_transaction: 1,
            request_client: None,
        }
    }
}

impl SqlEngine {
    pub fn execute(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        statement: &str,
    ) -> RsfResult<SqlOutcome> {
        match parse_statement(statement)? {
            ParsedStatement::CreateTable {
                table,
                primary_key,
                columns,
            } => {
                if self.tables.contains_key(&table) {
                    return Err(RsfError::new(
                        ErrorCode::InvalidInput,
                        format!("table {table} already exists"),
                    ));
                }
                let evidence = commit_sql_request(
                    router,
                    pg,
                    self.sql_request(format!("create-table:{table}:{primary_key}")),
                    FabricOperation::RelationalCatalog {
                        table: table.clone(),
                        item: "table".to_string(),
                    },
                )?;
                self.create_table_with_columns(table.clone(), primary_key, columns)?;
                self.schema_version = evidence.version;
                Ok(SqlOutcome::TableCreated {
                    table,
                    version: evidence.version,
                    evidence,
                })
            }
            ParsedStatement::CreateIndex { table, column } => {
                self.validate_index_column(&table, &column)?;
                let evidence = commit_sql_request(
                    router,
                    pg,
                    self.sql_request(format!("create-index:{table}:{column}")),
                    FabricOperation::RelationalCatalog {
                        table: table.clone(),
                        item: format!("index:{column}"),
                    },
                )?;
                self.create_index(&table, &column)?;
                self.schema_version = evidence.version;
                Ok(SqlOutcome::IndexCreated {
                    table,
                    column,
                    version: evidence.version,
                    evidence,
                })
            }
            ParsedStatement::Insert { table, columns } => {
                let primary_key = self.primary_key_value(&table, &columns)?;
                if self.transaction.is_some() {
                    let row = self.stage_transaction_insert(&table, primary_key, columns)?;
                    return Ok(SqlOutcome::Inserted {
                        row,
                        evidence: self.staged_transaction_evidence()?,
                    });
                }
                self.insert_with_evidence(router, pg, &table, primary_key, columns)
                    .map(|write| SqlOutcome::Inserted {
                        row: write.row,
                        evidence: SqlMutationEvidence::Committed(write.evidence),
                    })
            }
            ParsedStatement::Update {
                table,
                predicate,
                assignments,
            } => {
                let primary_key = self.primary_key_from_predicate(&table, &predicate)?;
                if self.transaction.is_some() {
                    let row = self.stage_transaction_update(&table, &primary_key, assignments)?;
                    return Ok(SqlOutcome::Updated {
                        row,
                        evidence: self.staged_transaction_evidence()?,
                    });
                }
                self.update_with_evidence(router, pg, &table, &primary_key, assignments)
                    .map(|write| SqlOutcome::Updated {
                        row: write.row,
                        evidence: SqlMutationEvidence::Committed(write.evidence),
                    })
            }
            ParsedStatement::Delete { table, predicate } => {
                if self.transaction.is_some() {
                    let primary_key = self.primary_key_from_predicate(&table, &predicate)?;
                    self.stage_transaction_delete(&table, &primary_key)?;
                    return Ok(SqlOutcome::Deleted {
                        table,
                        primary_key,
                        evidence: self.staged_transaction_evidence()?,
                    });
                }
                let primary_key = self.primary_key_from_predicate(&table, &predicate)?;
                self.delete_with_evidence(router, pg, &table, &primary_key)
                    .map(|evidence| SqlOutcome::Deleted {
                        table,
                        primary_key,
                        evidence: SqlMutationEvidence::Committed(evidence),
                    })
            }
            ParsedStatement::Select {
                table,
                projection,
                predicate,
                limit,
            } => self
                .execute_select(&table, projection, predicate, limit)
                .map(|rows| SqlOutcome::Selected { rows }),
            ParsedStatement::Begin => {
                self.begin_transaction(router)
                    .map(|(session_id, snapshot)| SqlOutcome::TransactionBegan {
                        session_id,
                        snapshot,
                    })
            }
            ParsedStatement::Savepoint { name } => {
                self.create_transaction_savepoint(&name).map(|staged| {
                    SqlOutcome::SavepointCreated {
                        session_id: self.active_transaction_session(),
                        name,
                        staged,
                    }
                })
            }
            ParsedStatement::RollbackToSavepoint { name } => self
                .rollback_transaction_to_savepoint(&name)
                .map(|staged| SqlOutcome::SavepointRolledBack {
                    session_id: self.active_transaction_session(),
                    name,
                    staged,
                }),
            ParsedStatement::Commit => {
                self.commit_transaction(router, pg)
                    .map(
                        |(session_id, evidence, mutations)| SqlOutcome::TransactionCommitted {
                            session_id,
                            evidence,
                            mutations,
                        },
                    )
            }
            ParsedStatement::Rollback => {
                self.rollback_transaction().map(|(session_id, discarded)| {
                    SqlOutcome::TransactionRolledBack {
                        session_id,
                        discarded,
                    }
                })
            }
        }
    }

    pub fn execute_with_request_client(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        statement: &str,
        request_client: impl Into<String>,
    ) -> RsfResult<SqlOutcome> {
        let previous = self.request_client.replace(request_client.into());
        let outcome = self.execute(router, pg, statement);
        self.request_client = previous;
        outcome
    }

    pub fn create_table(&mut self, name: impl Into<String>, primary_key: impl Into<String>) {
        let primary_key = primary_key.into();
        self.create_table_with_columns(name, primary_key.clone(), [primary_key])
            .expect("single-column table descriptor is valid");
    }

    pub fn create_table_with_columns(
        &mut self,
        name: impl Into<String>,
        primary_key: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> RsfResult<()> {
        self.create_table_with_profile(name, primary_key, columns, SqlLayoutProfile::Hybrid)
    }

    pub fn create_table_with_profile(
        &mut self,
        name: impl Into<String>,
        primary_key: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
        profile: SqlLayoutProfile,
    ) -> RsfResult<()> {
        let name = name.into();
        let primary_key = primary_key.into();
        let columns = columns.into_iter().map(Into::into).collect::<Vec<_>>();
        let descriptor = TableDescriptor {
            namespace: NamespaceName("default".to_string()),
            name: TableName(name),
            columns: columns
                .into_iter()
                .map(|name| ColumnDescriptor {
                    name,
                    ty: ColumnType::Text,
                    nullable: true,
                })
                .collect(),
            primary_key: vec![primary_key],
        };
        self.register_table_descriptor_with_profile(descriptor, profile)
    }

    pub fn register_catalog_table(
        &mut self,
        catalog: &CatalogEngine,
        namespace: &str,
        table: &str,
    ) -> RsfResult<()> {
        let descriptor = catalog.table(namespace, table).cloned().ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("catalog table {namespace}.{table} does not exist"),
            )
        })?;
        self.register_table_descriptor(descriptor)
    }

    pub fn register_table_descriptor(&mut self, descriptor: TableDescriptor) -> RsfResult<()> {
        self.register_table_descriptor_with_profile(descriptor, SqlLayoutProfile::Hybrid)
    }

    pub fn register_table_descriptor_with_profile(
        &mut self,
        descriptor: TableDescriptor,
        profile: SqlLayoutProfile,
    ) -> RsfResult<()> {
        let mut table = table_from_descriptor(descriptor)?;
        let decision = choose_table_layout(&table, profile);
        table.layout_profile = decision.profile;
        table.layout = decision.layout;
        table.page_size = decision.page_size;
        self.layout_decisions
            .insert(table.name.clone(), decision.clone());
        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    pub fn committed_kv_for_recovery(&self) -> KvEngine {
        self.kv.clone()
    }

    fn recovered_tables_for_checkpoint(&self) -> Vec<SqlRecoveredTable> {
        self.tables
            .values()
            .map(|table| SqlRecoveredTable {
                descriptor: table.descriptor.clone(),
                profile: table.layout_profile,
                constraints: table.constraints.clone(),
                secondary_indexes: table.secondary_indexes.clone(),
            })
            .collect()
    }

    fn committed_kv_versions_for_checkpoint(&self) -> Vec<(Vec<u8>, Vec<KvVersionedValue>)> {
        self.kv.scan_prefix_versions(b"")
    }

    pub fn recover_from_committed_kv(
        kv: KvEngine,
        tables: impl IntoIterator<Item = SqlRecoveredTable>,
    ) -> RsfResult<(Self, SqlRecoveryReport)> {
        let mut engine = Self {
            kv,
            ..Self::default()
        };
        for recovered in tables {
            engine.register_recovered_table(recovered)?;
        }
        let report = engine.rebuild_recovered_relational_state()?;
        // KV versions are the durable source for this recovery path. Rebuild
        // live page slots too, so subsequent UPDATE/DELETE redo has a base row.
        for row in engine.rows.values().cloned().collect::<Vec<_>>() {
            let key = logical_row_key(&row.table, &row.primary_key);
            let payload = engine
                .kv
                .get(&key)
                .expect("recovered live row has KV bytes")
                .value
                .clone();
            engine.apply_page_wal(
                &row.table,
                RedoOperation::InsertRow,
                row.version,
                checked_len_i32(&payload)?,
                key,
                Some(payload),
            )?;
        }
        Ok((engine, report))
    }

    pub fn rebuild_recovered_relational_state(&mut self) -> RsfResult<SqlRecoveryReport> {
        if self.transaction.is_some() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "cannot rebuild relational recovery state while a transaction is active",
            ));
        }

        self.rows.clear();
        self.primary = PrimaryIndex::default();
        self.tuples = TupleStore::default();
        self.constraints = ConstraintState::default();
        self.indexes = IndexStore::default();

        let mut recovered_rows = Vec::new();
        let mut row_versions = 0;
        let mut tombstones = 0;
        let mut highest_version = CommitVersion(0);

        for table in self.tables.values().cloned().collect::<Vec<_>>() {
            for (key, versions) in self
                .kv
                .scan_prefix_versions(table_row_prefix(&table.name).as_bytes())
            {
                let primary_key = primary_key_from_logical_row_key(&table.name, &key)?;
                let live_row = self.replay_recovered_tuple_versions(
                    &table,
                    &primary_key,
                    &versions,
                    &mut row_versions,
                    &mut tombstones,
                    &mut highest_version,
                )?;
                if let Some(row) = live_row {
                    recovered_rows.push(row);
                }
            }
        }

        for row in &recovered_rows {
            let table = self.table_record(&row.table)?.clone();
            self.constraints.add_referenced_key(
                &table.descriptor.name,
                &table.descriptor.primary_key,
                concrete_constraint_key(&table, row, &table.descriptor.primary_key)
                    .unwrap_or_else(|| vec![row.primary_key.clone()]),
            );
        }

        for row in recovered_rows {
            let table = self.table_record(&row.table)?.clone();
            self.validate_recovered_row(&table, &row)?;
            self.apply_primary_insert(
                &row.table,
                &row.primary_key,
                &row.columns,
                PlacementGroupId(0),
                row.version,
            )?;
            self.index_row(&row)?;
            add_unique_constraint_state_for(&mut self.constraints, &table, &row);
            self.rows
                .insert((row.table.clone(), row.primary_key.clone()), row);
        }

        Ok(SqlRecoveryReport {
            tables: self.tables.len(),
            row_versions,
            live_rows: self.rows.len(),
            tombstones,
            primary_index_entries: self.primary_index_len(),
            secondary_index_entries: self.secondary_index_entry_count(),
            unique_constraint_keys: self.unique_constraint_key_count(),
            referenced_constraint_keys: self.referenced_constraint_key_count(),
            highest_version,
        })
    }

    pub fn table_layout_decision(&self, table: &str) -> Option<&SqlLayoutDecision> {
        self.layout_decisions.get(table)
    }

    pub fn add_unique_constraint(
        &mut self,
        table: &str,
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> RsfResult<()> {
        let name = name.into();
        let columns = columns.into_iter().map(Into::into).collect::<Vec<_>>();
        let table_record = self.table_record(table)?;
        let rule = ConstraintRule::Unique {
            name,
            columns: columns.clone(),
        };
        ConstraintValidator::new(&table_record.descriptor, vec![rule.clone()])?;
        self.validate_existing_unique_constraint(table_record, &rule)?;
        self.tables
            .get_mut(table)
            .expect("table existence checked")
            .constraints
            .push(rule);
        let rows = self
            .rows
            .values()
            .filter(|row| row.table == table)
            .cloned()
            .collect::<Vec<_>>();
        for row in &rows {
            self.add_constraint_state_for_row(table, row)?;
        }
        Ok(())
    }

    pub fn add_foreign_key_constraint(
        &mut self,
        table: &str,
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
        referenced_table: impl Into<String>,
        referenced_columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> RsfResult<()> {
        let referenced_table = TableName(referenced_table.into());
        let columns = columns.into_iter().map(Into::into).collect::<Vec<_>>();
        let referenced_columns = referenced_columns
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let table_record = self.table_record(table)?;
        if !self.tables.contains_key(&referenced_table.0) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("referenced table {} does not exist", referenced_table.0),
            ));
        }
        let rule = ConstraintRule::ForeignKey {
            name: name.into(),
            columns,
            referenced_table,
            referenced_columns,
        };
        ConstraintValidator::new(&table_record.descriptor, vec![rule.clone()])?;
        self.validate_existing_foreign_key_constraint(table_record, &rule)?;
        self.tables
            .get_mut(table)
            .expect("table existence checked")
            .constraints
            .push(rule);
        Ok(())
    }

    pub fn create_index(&mut self, table: &str, column: &str) -> RsfResult<()> {
        let Some(table_record) = self.tables.get_mut(table) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown table {table}"),
            ));
        };
        ensure_declared_column(table_record, column)?;
        if column == table_record.primary_key {
            return Ok(());
        }
        let descriptor = secondary_index_descriptor(table_record, column);
        IndexPlanner::new(&table_record.descriptor, vec![descriptor.clone()])?;
        if !table_record
            .secondary_indexes
            .iter()
            .any(|indexed| indexed == column)
        {
            table_record.secondary_indexes.push(column.to_string());
        }

        self.rebuild_index(table, &descriptor)
    }

    pub fn insert(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        table: &str,
        primary_key: impl Into<String>,
        columns: BTreeMap<String, String>,
    ) -> RsfResult<Row> {
        Ok(self
            .insert_with_evidence(router, pg, table, primary_key, columns)?
            .row)
    }

    pub fn insert_with_evidence(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        table: &str,
        primary_key: impl Into<String>,
        columns: BTreeMap<String, String>,
    ) -> RsfResult<SqlWriteResult> {
        if !self.tables.contains_key(table) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown table {table}"),
            ));
        }
        self.validate_row_columns(table, &columns)?;

        let primary_key = primary_key.into();
        if self.select_pk(table, &primary_key).is_some() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("row {table}/{primary_key} is already present"),
            ));
        }
        self.validate_constraints_for_values(table, &primary_key, &columns)?;
        let logical_key = format!("table/{table}/pk/{primary_key}").into_bytes();
        let request_payload =
            self.encode_row_payload(table, &primary_key, &columns, CommitVersion(0))?;
        self.validate_page_payload(table, &logical_key, &request_payload)?;
        let evidence = commit_sql_request(
            router,
            pg,
            self.sql_request(format!(
                "insert:{table}:{primary_key}:{}",
                stable_hash(&request_payload)
            )),
            FabricOperation::RelationalMutation {
                table: table.to_string(),
                key: logical_key.clone(),
            },
        )?;
        let mut committed = self.clone();
        committed.apply_transaction_insert(table, &primary_key, &columns, pg, evidence.version)?;
        let row = committed
            .select_pk(table, &primary_key)
            .cloned()
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "committed SQL insert did not materialize a primary row",
                )
            })?;
        *self = committed;
        Ok(SqlWriteResult { row, evidence })
    }

    pub fn update(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        table: &str,
        primary_key: &str,
        assignments: BTreeMap<String, String>,
    ) -> RsfResult<Row> {
        Ok(self
            .update_with_evidence(router, pg, table, primary_key, assignments)?
            .row)
    }

    pub fn update_with_evidence(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        table: &str,
        primary_key: &str,
        assignments: BTreeMap<String, String>,
    ) -> RsfResult<SqlWriteResult> {
        let existing = self
            .select_pk(table, primary_key)
            .cloned()
            .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "row is not present"))?;
        self.validate_assignment_columns(table, &assignments)?;
        let mut columns = existing.columns.clone();
        for (column, value) in assignments {
            columns.insert(column, value);
        }
        self.validate_constraints_for_update(table, primary_key, &existing, &columns)?;
        let logical_key = format!("table/{table}/pk/{primary_key}").into_bytes();
        let request_payload =
            self.encode_row_payload(table, primary_key, &columns, CommitVersion(0))?;
        self.validate_page_payload(table, &logical_key, &request_payload)?;
        let evidence = commit_sql_request(
            router,
            pg,
            self.sql_request(format!(
                "update:{table}:{primary_key}:{}",
                stable_hash(&request_payload)
            )),
            FabricOperation::RelationalMutation {
                table: table.to_string(),
                key: logical_key.clone(),
            },
        )?;
        let mut committed = self.clone();
        committed.apply_transaction_update(table, primary_key, &columns, pg, evidence.version)?;
        let row = committed
            .select_pk(table, primary_key)
            .cloned()
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "committed SQL update did not materialize a primary row",
                )
            })?;
        *self = committed;
        Ok(SqlWriteResult { row, evidence })
    }

    pub fn delete(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        table: &str,
        primary_key: &str,
    ) -> RsfResult<()> {
        self.delete_with_evidence(router, pg, table, primary_key)?;
        Ok(())
    }

    pub fn delete_with_evidence(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        table: &str,
        primary_key: &str,
    ) -> RsfResult<CommitEvidence> {
        if self.select_pk(table, primary_key).is_none() {
            return Err(RsfError::new(ErrorCode::InvalidInput, "row is not present"));
        }
        let logical_key = format!("table/{table}/pk/{primary_key}").into_bytes();
        let evidence = commit_sql_request(
            router,
            pg,
            self.sql_request(format!("delete:{table}:{primary_key}")),
            FabricOperation::RelationalMutation {
                table: table.to_string(),
                key: logical_key.clone(),
            },
        )?;
        let mut committed = self.clone();
        committed.apply_transaction_delete(table, primary_key, evidence.version)?;
        *self = committed;
        Ok(evidence)
    }

    pub fn select_pk(&self, table: &str, primary_key: &str) -> Option<&Row> {
        let table_record = self.tables.get(table)?;
        match self
            .primary
            .lookup(&table_record.primary_descriptor, &[primary_key.to_string()])
            .ok()?
        {
            PrimaryDecision::Found(_) => {
                self.rows.get(&(table.to_string(), primary_key.to_string()))
            }
            PrimaryDecision::Missing { .. }
            | PrimaryDecision::Inserted(_)
            | PrimaryDecision::Updated { .. }
            | PrimaryDecision::Deleted(_)
            | PrimaryDecision::Rejected { .. } => None,
        }
    }

    pub fn select_pk_at(
        &self,
        table: &str,
        primary_key: &str,
        snapshot: CommitVersion,
    ) -> Option<Row> {
        let table_record = self.tables.get(table)?;
        match self
            .tuples
            .read_visible(&tuple_key(table, primary_key), snapshot)
        {
            TupleDecision::Visible { version, .. } => self
                .row_from_tuple(table_record, primary_key, &version)
                .ok(),
            TupleDecision::Invisible { .. } => None,
            TupleDecision::Inserted(_)
            | TupleDecision::Updated { .. }
            | TupleDecision::Deleted(_)
            | TupleDecision::Rejected { .. } => None,
        }
    }

    pub fn row_count(&self) -> usize {
        self.tables
            .keys()
            .map(|table| self.committed_rows_for_table(table).len())
            .sum()
    }

    pub fn tuple_version_count(&self) -> usize {
        self.tuples.version_count()
    }

    pub fn primary_index_len(&self) -> usize {
        self.primary.len()
    }

    pub fn secondary_index_entry_count(&self) -> usize {
        self.indexes.entry_count()
    }

    pub fn query_report_count(&self) -> usize {
        self.query_reports.len()
    }

    pub fn last_query_report(&self) -> Option<&SqlQueryReport> {
        self.query_reports.last()
    }

    pub fn prepare_statement(
        &mut self,
        statement: &str,
        sql: &str,
        parameter_types: Vec<PreparedParamType>,
    ) -> Vec<PortalDecision> {
        self.portals
            .parse(statement, sql, self.schema_version, parameter_types)
    }

    pub fn bind_portal(
        &mut self,
        portal: &str,
        statement: &str,
        parameters: Vec<PreparedValue>,
    ) -> PortalDecision {
        self.portals
            .bind(portal, statement, self.schema_version, parameters)
    }

    pub fn describe_portal(&mut self, portal: &str) -> PortalDecision {
        self.portals.describe(portal)
    }

    pub fn execute_portal(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        portal: &str,
        deadline_ms: u64,
    ) -> RsfResult<SqlPortalExecution> {
        let statement = self.portals.bound_sql(portal)?;
        let previous_report_count = self.query_reports.len();
        let outcome = self.execute(router, pg, &statement)?;
        let elapsed_ms = self.portal_elapsed_ms(previous_report_count);
        let rows = outcome_row_count(&outcome);
        let decision = self
            .portals
            .mark_executed(portal, deadline_ms, elapsed_ms, rows);
        Ok(SqlPortalExecution { decision, outcome })
    }

    pub fn cancel_portal(&mut self, portal: &str, reason: impl Into<String>) -> PortalDecision {
        self.portals.cancel(portal, reason)
    }

    pub fn close_portal(&mut self, portal: &str) -> PortalDecision {
        self.portals.close_portal(portal)
    }

    pub fn close_statement(&mut self, statement: &str) -> PortalDecision {
        self.portals.close_statement(statement)
    }

    pub fn prepared_schema_version(&self) -> CommitVersion {
        self.schema_version
    }

    pub fn transaction_active(&self) -> bool {
        self.transaction.is_some()
    }

    pub fn analyze_table(
        &mut self,
        table: &str,
        version: CommitVersion,
    ) -> RsfResult<TableEstimate> {
        let table_record = self.table_record(table)?.clone();
        let sample = self.table_sample(&table_record, version)?;
        self.stats.publish(&table_record.descriptor, sample)
    }

    pub fn stats_estimate(&self, table: &str) -> Option<&TableEstimate> {
        self.tables
            .get(table)
            .and_then(|table_record| self.stats.estimate(&table_record.descriptor.name))
    }

    pub fn stats_freshness(
        &self,
        table: &str,
        catalog_version: CommitVersion,
        max_lag: u64,
    ) -> RsfResult<Freshness> {
        let table_record = self.table_record(table)?;
        self.stats
            .freshness(&table_record.descriptor.name, catalog_version, max_lag)
    }

    pub fn vacuum_table(
        &mut self,
        table: &str,
        oldest_snapshot: CommitVersion,
        min_versions_to_keep: usize,
    ) -> RsfResult<SqlVacuumReport> {
        self.table_record(table)?;
        let policy = VacuumPolicy::new(oldest_snapshot, min_versions_to_keep)?;
        let planner = VacuumPlanner;
        let chains = self.tuples.chains_for_table(table);
        let mut report = SqlVacuumReport {
            table: table.to_string(),
            chains: 0,
            pruned_versions: 0,
            retained_versions: 0,
            reclaimed_bytes: 0,
            rejections: Vec::new(),
        };
        for chain in chains {
            match planner.plan(&chain, &policy) {
                VacuumDecision::Planned(plan) => {
                    report.chains += 1;
                    report.pruned_versions += plan.pruned_versions.len();
                    report.retained_versions += plan.retained_versions.len();
                    report.reclaimed_bytes += u64::from(plan.reclaimed_bytes);
                    if !plan.pruned_versions.is_empty() {
                        self.tuples
                            .replace_chain_versions(&plan.key, plan.retained_versions)?;
                    }
                }
                VacuumDecision::Rejected { key, reason } => {
                    report.rejections.push(format!(
                        "{}:{}:{reason}",
                        key.table,
                        String::from_utf8_lossy(&key.primary_key)
                    ));
                }
            }
        }
        Ok(report)
    }

    pub fn unique_constraint_key_count(&self) -> usize {
        self.constraints.unique_key_count()
    }

    pub fn referenced_constraint_key_count(&self) -> usize {
        self.constraints.referenced_key_count()
    }

    pub fn select_eq(&self, table: &str, column: &str, value: &str) -> Vec<Row> {
        let Some(table_record) = self.tables.get(table) else {
            return Vec::new();
        };
        if column == table_record.primary_key {
            return self
                .physical_primary_row(table_record, value)
                .into_iter()
                .collect::<Vec<_>>();
        }
        if ensure_declared_column(table_record, column).is_err() {
            return Vec::new();
        }
        let descriptor = secondary_index_descriptor(table_record, column);
        let row = RowImage {
            primary_key: value.to_string(),
            values: BTreeMap::from([(column.to_string(), value.to_string())]),
        };
        let Ok(key) = encode_index_key(&descriptor, &row) else {
            return Vec::new();
        };
        self.indexes
            .lookup(&descriptor.name, &key)
            .into_iter()
            .filter_map(|primary_key| self.physical_row(table_record, &primary_key))
            .collect()
    }

    fn execute_select(
        &mut self,
        table: &str,
        projection: Projection,
        predicate: Option<Predicate>,
        limit: Option<usize>,
    ) -> RsfResult<Vec<Row>> {
        let table_record = self.table_record(table)?.clone();
        let requested_projection = projection.clone();
        let typed_predicate = type_select_predicate(&table_record, predicate)?;
        let logical = QueryPlanner.select(
            &table_record.descriptor,
            projection_with_primary(&table_record, projection),
            typed_predicate,
            limit,
        )?;
        let physical = PhysicalPlanner.plan(
            &table_record.descriptor,
            &logical,
            &PhysicalPlanningContext {
                stats: self.table_stats(&table_record),
                indexes: self.secondary_index_descriptors(&table_record),
                stream_threshold_bytes: 64 * 1024,
                cacheable: false,
            },
        )?;
        let execution = PhysicalExecutor.execute(&physical)?;
        let candidates = self.query_candidates(&table_record, &logical, &physical)?;
        let result = QueryExecutor.execute(&logical, &candidates)?;
        self.query_reports.push(SqlQueryReport {
            logical,
            physical,
            execution,
        });
        let mut rows = result
            .rows
            .into_iter()
            .map(|row| row_from_query_row(&table_record, row))
            .collect::<RsfResult<Vec<_>>>()?;
        apply_requested_projection(&mut rows, &requested_projection)?;
        Ok(rows)
    }

    fn begin_transaction(
        &mut self,
        router: &impl FabricRouterPort,
    ) -> RsfResult<(String, CommitVersion)> {
        if self.transaction.is_some() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SQL transaction already active",
            ));
        }
        let sequence = self.next_transaction;
        self.next_transaction = self.next_transaction.saturating_add(1);
        let session_id = format!("sql-txn-{sequence}");
        let snapshot = router.latest_version();
        self.transaction = Some(SqlTransactionState {
            id: transaction_id_from_sequence(sequence),
            session_id: session_id.clone(),
            snapshot,
            staged: BTreeMap::new(),
            savepoints: BTreeMap::new(),
        });
        Ok((session_id, snapshot))
    }

    fn create_transaction_savepoint(&mut self, name: &str) -> RsfResult<usize> {
        validate_savepoint_name(name)?;
        let Some(transaction) = self.transaction.as_mut() else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SAVEPOINT requires an active SQL transaction",
            ));
        };
        transaction
            .savepoints
            .insert(name.to_string(), transaction.staged.clone());
        Ok(transaction.staged.len())
    }

    fn rollback_transaction_to_savepoint(&mut self, name: &str) -> RsfResult<usize> {
        validate_savepoint_name(name)?;
        let Some(transaction) = self.transaction.as_mut() else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "ROLLBACK TO requires an active SQL transaction",
            ));
        };
        let Some(staged) = transaction.savepoints.get(name).cloned() else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("SQL transaction has no savepoint {name}"),
            ));
        };
        transaction.staged = staged;
        Ok(transaction.staged.len())
    }

    fn rollback_transaction(&mut self) -> RsfResult<(String, usize)> {
        let Some(transaction) = self.transaction.take() else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "ROLLBACK requires an active SQL transaction",
            ));
        };
        Ok((transaction.session_id, transaction.staged.len()))
    }

    fn staged_transaction_evidence(&self) -> RsfResult<SqlMutationEvidence> {
        let Some(transaction) = self.transaction.as_ref() else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SQL staged mutation requires an active transaction",
            ));
        };
        Ok(SqlMutationEvidence::Staged {
            session_id: transaction.session_id.clone(),
            snapshot: transaction.snapshot,
        })
    }

    fn commit_transaction(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
    ) -> RsfResult<(String, CommitEvidence, usize)> {
        let Some(transaction) = self.transaction.clone() else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "COMMIT requires an active SQL transaction",
            ));
        };
        self.validate_transaction_commit(&transaction)?;
        let mutations = transaction.staged.len();
        let evidence = commit_sql_request(
            router,
            pg,
            self.sql_request(format!(
                "transaction:{}",
                transaction_id_number(&transaction.id)
            )),
            FabricOperation::TransactionCommit {
                transaction: transaction.id.0,
            },
        )?;
        let mut committed = self.clone();
        committed.transaction = None;
        for ((table, primary_key), mutation) in &transaction.staged {
            committed.apply_transaction_mutation(
                table,
                primary_key,
                mutation,
                pg,
                evidence.version,
            )?;
        }
        *self = committed;
        Ok((transaction.session_id, evidence, mutations))
    }

    fn stage_transaction_insert(
        &mut self,
        table: &str,
        primary_key: String,
        columns: BTreeMap<String, String>,
    ) -> RsfResult<Row> {
        self.require_active_transaction()?;
        self.validate_row_columns(table, &columns)?;
        if self.transaction_row(table, &primary_key).is_some() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("row {table}/{primary_key} is already present"),
            ));
        }
        self.validate_transaction_candidate(
            table,
            &primary_key,
            Some(SqlTransactionMutation::Set(columns.clone())),
        )?;
        self.transaction
            .as_mut()
            .expect("transaction existence checked")
            .staged
            .insert(
                (table.to_string(), primary_key.clone()),
                SqlTransactionMutation::Set(columns.clone()),
            );
        Ok(Row {
            table: table.to_string(),
            primary_key,
            columns,
            version: self
                .transaction
                .as_ref()
                .expect("transaction existence checked")
                .snapshot,
        })
    }

    fn stage_transaction_update(
        &mut self,
        table: &str,
        primary_key: &str,
        assignments: BTreeMap<String, String>,
    ) -> RsfResult<Row> {
        self.require_active_transaction()?;
        self.validate_assignment_columns(table, &assignments)?;
        let Some(mut row) = self.transaction_row(table, primary_key) else {
            return Err(RsfError::new(ErrorCode::InvalidInput, "row is not present"));
        };
        for (column, value) in assignments {
            row.columns.insert(column, value);
        }
        self.validate_transaction_candidate(
            table,
            primary_key,
            Some(SqlTransactionMutation::Set(row.columns.clone())),
        )?;
        self.transaction
            .as_mut()
            .expect("transaction existence checked")
            .staged
            .insert(
                (table.to_string(), primary_key.to_string()),
                SqlTransactionMutation::Set(row.columns.clone()),
            );
        Ok(row)
    }

    fn stage_transaction_delete(&mut self, table: &str, primary_key: &str) -> RsfResult<()> {
        self.require_active_transaction()?;
        if self.transaction_row(table, primary_key).is_none() {
            return Err(RsfError::new(ErrorCode::InvalidInput, "row is not present"));
        }
        self.validate_transaction_candidate(
            table,
            primary_key,
            Some(SqlTransactionMutation::Delete),
        )?;
        self.transaction
            .as_mut()
            .expect("transaction existence checked")
            .staged
            .insert(
                (table.to_string(), primary_key.to_string()),
                SqlTransactionMutation::Delete,
            );
        Ok(())
    }

    fn require_active_transaction(&self) -> RsfResult<()> {
        if self.transaction.is_some() {
            Ok(())
        } else {
            Err(RsfError::new(
                ErrorCode::InvalidInput,
                "statement requires an active SQL transaction",
            ))
        }
    }

    fn active_transaction_session(&self) -> String {
        self.transaction
            .as_ref()
            .map(|transaction| transaction.session_id.clone())
            .unwrap_or_default()
    }

    fn transaction_row(&self, table: &str, primary_key: &str) -> Option<Row> {
        let key = (table.to_string(), primary_key.to_string());
        if let Some(transaction) = &self.transaction
            && let Some(mutation) = transaction.staged.get(&key)
        {
            return match mutation {
                SqlTransactionMutation::Set(columns) => Some(Row {
                    table: table.to_string(),
                    primary_key: primary_key.to_string(),
                    columns: columns.clone(),
                    version: transaction.snapshot,
                }),
                SqlTransactionMutation::Delete => None,
            };
        }
        let table_record = self.tables.get(table)?;
        self.physical_primary_row(table_record, primary_key)
    }

    fn transaction_rows_for_table(&self, table: &str) -> Vec<Row> {
        let mut rows = self
            .committed_rows_for_table(table)
            .into_iter()
            .map(|row| (row.primary_key.clone(), row))
            .collect::<BTreeMap<_, _>>();
        if let Some(transaction) = &self.transaction {
            for ((staged_table, primary_key), mutation) in &transaction.staged {
                if staged_table != table {
                    continue;
                }
                match mutation {
                    SqlTransactionMutation::Set(columns) => {
                        rows.insert(
                            primary_key.clone(),
                            Row {
                                table: table.to_string(),
                                primary_key: primary_key.clone(),
                                columns: columns.clone(),
                                version: transaction.snapshot,
                            },
                        );
                    }
                    SqlTransactionMutation::Delete => {
                        rows.remove(primary_key);
                    }
                }
            }
        }
        rows.into_values().collect()
    }

    fn validate_transaction_candidate(
        &self,
        table: &str,
        primary_key: &str,
        candidate: Option<SqlTransactionMutation>,
    ) -> RsfResult<()> {
        let transaction = self.transaction.as_ref().ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "statement requires an active SQL transaction",
            )
        })?;
        let version = transaction.snapshot.checked_next()?;
        let mut staged = transaction.staged.clone();
        let key = (table.to_string(), primary_key.to_string());
        if let Some(candidate) = candidate {
            staged.insert(key, candidate);
        } else {
            staged.remove(&key);
        }
        let mut clone = self.clone_without_transaction();
        for ((table, primary_key), mutation) in staged {
            clone.apply_transaction_mutation(
                &table,
                &primary_key,
                &mutation,
                PlacementGroupId(0),
                version,
            )?;
        }
        Ok(())
    }

    fn validate_transaction_commit(&self, transaction: &SqlTransactionState) -> RsfResult<()> {
        let mut clone = self.clone_without_transaction();
        for ((table, primary_key), mutation) in &transaction.staged {
            clone.apply_transaction_mutation(
                table,
                primary_key,
                mutation,
                PlacementGroupId(0),
                transaction.snapshot.checked_next()?,
            )?;
        }
        Ok(())
    }

    fn clone_without_transaction(&self) -> Self {
        let mut clone = self.clone();
        clone.transaction = None;
        clone
    }

    fn apply_transaction_mutation(
        &mut self,
        table: &str,
        primary_key: &str,
        mutation: &SqlTransactionMutation,
        pg: PlacementGroupId,
        version: CommitVersion,
    ) -> RsfResult<()> {
        match mutation {
            SqlTransactionMutation::Set(columns) => {
                if self.select_pk(table, primary_key).is_some() {
                    self.apply_transaction_update(table, primary_key, columns, pg, version)
                } else {
                    self.apply_transaction_insert(table, primary_key, columns, pg, version)
                }
            }
            SqlTransactionMutation::Delete => {
                self.apply_transaction_delete(table, primary_key, version)
            }
        }
    }

    fn apply_transaction_insert(
        &mut self,
        table: &str,
        primary_key: &str,
        columns: &BTreeMap<String, String>,
        pg: PlacementGroupId,
        version: CommitVersion,
    ) -> RsfResult<()> {
        if self.select_pk(table, primary_key).is_some() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("row {table}/{primary_key} is already present"),
            ));
        }
        self.validate_row_columns(table, columns)?;
        self.validate_constraints_for_values(table, primary_key, columns)?;
        let logical_key = logical_row_key(table, primary_key);
        let row_payload = self.encode_row_payload(table, primary_key, columns, version)?;
        self.apply_page_wal(
            table,
            RedoOperation::InsertRow,
            version,
            checked_len_i32(&row_payload)?,
            logical_key.clone(),
            Some(row_payload.clone()),
        )?;
        self.kv
            .apply_committed_set(logical_key, row_payload.clone(), version);
        self.apply_primary_insert(table, primary_key, columns, pg, version)?;
        self.apply_tuple_insert(table, primary_key, row_payload, version)?;
        let row = Row {
            table: table.to_string(),
            primary_key: primary_key.to_string(),
            columns: columns.clone(),
            version,
        };
        self.rows
            .insert((row.table.clone(), row.primary_key.clone()), row.clone());
        self.index_row(&row)?;
        self.add_constraint_state_for_row(table, &row)
    }

    fn apply_transaction_update(
        &mut self,
        table: &str,
        primary_key: &str,
        columns: &BTreeMap<String, String>,
        pg: PlacementGroupId,
        version: CommitVersion,
    ) -> RsfResult<()> {
        let existing = self
            .select_pk(table, primary_key)
            .cloned()
            .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "row is not present"))?;
        self.validate_row_columns(table, columns)?;
        self.validate_constraints_for_update(table, primary_key, &existing, columns)?;
        self.deindex_row(&existing)?;
        let logical_key = logical_row_key(table, primary_key);
        let row_payload = self.encode_row_payload(table, primary_key, columns, version)?;
        let previous_bytes = self
            .kv
            .get(&logical_key)
            .map(|value| value.value.len())
            .unwrap_or_default();
        self.apply_page_wal(
            table,
            RedoOperation::UpdateRow,
            version,
            checked_len_i32(&row_payload)? - checked_usize_i32(previous_bytes)?,
            logical_key.clone(),
            Some(row_payload.clone()),
        )?;
        self.kv
            .apply_committed_set(logical_key, row_payload.clone(), version);
        self.apply_primary_update(table, primary_key, columns, pg, version)?;
        self.apply_tuple_update(table, primary_key, row_payload, version)?;
        let row = Row {
            table: table.to_string(),
            primary_key: primary_key.to_string(),
            columns: columns.clone(),
            version,
        };
        self.rows
            .insert((row.table.clone(), row.primary_key.clone()), row.clone());
        self.index_row(&row)?;
        self.remove_constraint_state_for_row(table, &existing)?;
        self.add_constraint_state_for_row(table, &row)
    }

    fn apply_transaction_delete(
        &mut self,
        table: &str,
        primary_key: &str,
        version: CommitVersion,
    ) -> RsfResult<()> {
        let Some(row) = self
            .rows
            .remove(&(table.to_string(), primary_key.to_string()))
        else {
            return Err(RsfError::new(ErrorCode::InvalidInput, "row is not present"));
        };
        let logical_key = logical_row_key(table, primary_key);
        let previous_bytes = self
            .kv
            .get(&logical_key)
            .map(|value| value.value.len())
            .unwrap_or_default();
        self.apply_page_wal(
            table,
            RedoOperation::DeleteRow,
            version,
            -checked_usize_i32(previous_bytes)?,
            logical_key.clone(),
            None,
        )?;
        self.kv.apply_committed_clear(logical_key, version);
        self.apply_primary_delete(table, primary_key)?;
        self.apply_tuple_delete(table, primary_key, version)?;
        self.deindex_row(&row)?;
        self.remove_constraint_state_for_row(table, &row)
    }

    fn portal_elapsed_ms(&self, previous_report_count: usize) -> u64 {
        if self.query_reports.len() > previous_report_count {
            self.last_query_report()
                .map(|report| report.execution.stages.len().max(1) as u64)
                .unwrap_or(1)
        } else {
            1
        }
    }

    fn query_candidates(
        &self,
        table: &Table,
        logical: &QueryPlan,
        physical: &PhysicalPlan,
    ) -> RsfResult<Vec<QueryRow>> {
        let access = physical
            .operators
            .iter()
            .find_map(|operator| match operator.access {
                AccessPath::CacheLookup | AccessPath::StreamRead { .. } => None,
                _ => Some(&operator.access),
            })
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    "physical query plan has no row-producing access operator",
                )
            })?;

        let rows = match access {
            AccessPath::PrimaryKeyLookup { column } => {
                let Some(value) = equality_text_value(logical.predicate.as_ref(), column) else {
                    return Ok(Vec::new());
                };
                self.transaction_row(&table.name, &value)
                    .into_iter()
                    .collect::<Vec<_>>()
            }
            AccessPath::SecondaryIndexLookup { column, .. } => {
                let Some(value) = equality_text_value(logical.predicate.as_ref(), column) else {
                    return Ok(Vec::new());
                };
                self.transaction_rows_for_table(&table.name)
                    .into_iter()
                    .filter(|row| {
                        row.columns
                            .get(column)
                            .is_some_and(|actual| actual == &value)
                    })
                    .collect()
            }
            AccessPath::RangeScan => self.transaction_rows_for_table(&table.name),
            AccessPath::CacheLookup | AccessPath::StreamRead { .. } => Vec::new(),
        };

        rows.into_iter()
            .map(|row| query_row_from_row(table, &row))
            .collect()
    }

    fn table_stats(&self, table: &Table) -> TableStats {
        if let Some(estimate) = self.stats.estimate(&table.descriptor.name)
            && estimate.rows > 0
        {
            return TableStats {
                table: estimate.table.clone(),
                rows: estimate.rows,
                average_row_bytes: estimate.average_row_bytes().max(1),
            };
        }
        let rows = self
            .committed_rows_for_table(&table.name)
            .into_iter()
            .collect::<Vec<_>>();
        let row_count = rows.len() as u64;
        let total_bytes = rows
            .iter()
            .map(|row| {
                row.columns
                    .iter()
                    .map(|(column, value)| column.len() + value.len())
                    .sum::<usize>()
            })
            .sum::<usize>() as u64;
        TableStats {
            table: table.descriptor.name.clone(),
            rows: row_count,
            average_row_bytes: if row_count == 0 {
                1
            } else {
                (total_bytes / row_count).max(1)
            },
        }
    }

    fn table_sample(&self, table: &Table, version: CommitVersion) -> RsfResult<TableSample> {
        let rows = self
            .committed_rows_for_table(&table.name)
            .into_iter()
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("statistics sample for table {} has no rows", table.name),
            ));
        }
        let total_bytes = rows
            .iter()
            .map(|row| {
                row.columns
                    .iter()
                    .map(|(column, value)| column.len() + value.len())
                    .sum::<usize>()
            })
            .sum::<usize>() as u64;
        let mut columns = Vec::new();
        for column in &table.descriptor.columns {
            let mut values = BTreeMap::<Option<String>, u64>::new();
            for row in &rows {
                *values
                    .entry(row.columns.get(&column.name).cloned())
                    .or_default() += 1;
            }
            columns.push(ColumnSample {
                column: column.name.clone(),
                values: values
                    .into_iter()
                    .map(|(value, rows)| ValueSample { value, rows })
                    .collect(),
            });
        }
        Ok(TableSample {
            table: table.descriptor.name.clone(),
            rows: rows.len() as u64,
            total_bytes: total_bytes.max(rows.len() as u64),
            version,
            columns,
        })
    }

    fn secondary_index_descriptors(&self, table: &Table) -> Vec<IndexDescriptor> {
        table
            .secondary_indexes
            .iter()
            .map(|column| secondary_index_descriptor(table, column))
            .collect()
    }

    fn primary_key_value(
        &self,
        table: &str,
        columns: &BTreeMap<String, String>,
    ) -> RsfResult<String> {
        let Some(table_record) = self.tables.get(table) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown table {table}"),
            ));
        };
        columns
            .get(&table_record.primary_key)
            .cloned()
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    format!(
                        "INSERT into {table} requires primary key column {}",
                        table_record.primary_key
                    ),
                )
            })
    }

    fn primary_key_from_predicate(
        &self,
        table: &str,
        predicate: &EqualityPredicate,
    ) -> RsfResult<String> {
        let Some(table_record) = self.tables.get(table) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown table {table}"),
            ));
        };
        if predicate.column != table_record.primary_key {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "mutation predicate for {table} must target primary key column {}",
                    table_record.primary_key
                ),
            ));
        }
        Ok(predicate.value.clone())
    }

    fn validate_row_columns(
        &self,
        table: &str,
        columns: &BTreeMap<String, String>,
    ) -> RsfResult<()> {
        let Some(table_record) = self.tables.get(table) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown table {table}"),
            ));
        };
        for column in columns.keys() {
            ensure_declared_column(table_record, column)?;
        }
        Ok(())
    }

    fn validate_assignment_columns(
        &self,
        table: &str,
        assignments: &BTreeMap<String, String>,
    ) -> RsfResult<()> {
        let Some(table_record) = self.tables.get(table) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown table {table}"),
            ));
        };
        for column in assignments.keys() {
            ensure_declared_column(table_record, column)?;
        }
        Ok(())
    }

    fn validate_index_column(&self, table: &str, column: &str) -> RsfResult<()> {
        let Some(table_record) = self.tables.get(table) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown table {table}"),
            ));
        };
        ensure_declared_column(table_record, column)
    }

    pub fn snapshot(&self, router: &impl FabricRouterPort) -> ReadSnapshot {
        self.kv.snapshot(router)
    }

    pub fn encoded_row_at(
        &self,
        table: &str,
        primary_key: &str,
        snapshot: ReadSnapshot,
    ) -> Option<&[u8]> {
        let key = format!("table/{table}/pk/{primary_key}");
        self.kv
            .get_at(key.as_bytes(), snapshot)
            .map(|value| value.value.as_slice())
    }

    pub fn page_wal_report(&self) -> SqlPageWalReport {
        let (checkpoint_lsn, checkpoint_bytes, checkpoint_live_rows, checkpoint_tombstones) =
            match self.pages.checkpoint() {
                PageDecision::Checkpoint(checkpoint) => (
                    checkpoint.high_lsn,
                    checkpoint.bytes,
                    checkpoint.live_rows,
                    checkpoint.tombstones,
                ),
                PageDecision::Applied(_)
                | PageDecision::Read(_)
                | PageDecision::Replay(_)
                | PageDecision::Rejected { .. } => (CommitVersion(0), 0, 0, 0),
            };
        SqlPageWalReport {
            pages: self.pages.page_count(),
            page_redo: self.pages.redo_count(),
            wal_entries: self.wal.appended_count(),
            durable_lsn: self.wal.durable_lsn(),
            checkpoint_lsn,
            checkpoint_bytes,
            checkpoint_live_rows,
            checkpoint_tombstones,
            buffer_resident: self.buffer.resident_count(),
            ordered_kv_tables: self
                .layout_decisions
                .values()
                .filter(|decision| decision.layout == SqlTableLayout::OrderedKv)
                .count(),
            page_wal_tables: self
                .layout_decisions
                .values()
                .filter(|decision| decision.layout == SqlTableLayout::PageWal)
                .count(),
        }
    }

    pub fn page_wal_snapshot(&self) -> SqlPageWalSnapshot {
        SqlPageWalSnapshot {
            page_images: self.page_wal_base_images.values().cloned().collect(),
            redo_records: self.page_wal_redo_records.clone(),
        }
    }

    pub fn recover_page_wal_snapshot(
        &mut self,
        snapshot: SqlPageWalSnapshot,
    ) -> RsfResult<ReplayReport> {
        let (pages, report) =
            PageServer::recover_with_report(&snapshot.page_images, &snapshot.redo_records)
                .map_err(page_replay_error)?;
        let mut table_pages: BTreeMap<String, BTreeSet<PageId>> = BTreeMap::new();
        let mut row_pages = BTreeMap::new();
        for page in pages.page_ids() {
            self.table_record(&page.table)?;
            let image = pages
                .read_image(&page, CommitVersion(0))
                .map_err(|reason| RsfError::new(ErrorCode::CorruptionDetected, reason))?;
            let prefix = format!("table/{}/pk/", page.table).into_bytes();
            for slot in image
                .slots()
                .map_err(|reason| RsfError::new(ErrorCode::CorruptionDetected, reason))?
            {
                if !slot.row_key.starts_with(&prefix) {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "foreign row key in recovered SQL page",
                    ));
                }
                if !slot.deleted && row_pages.insert(slot.row_key, page.clone()).is_some() {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "duplicate live row in recovered SQL pages",
                    ));
                }
            }
            table_pages
                .entry(page.table.clone())
                .or_default()
                .insert(page);
        }
        let next_page_lsn = report.high_lsn.0.checked_add(1).ok_or_else(|| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "recovered SQL page LSN exhausted",
            )
        })?;
        let base_images = snapshot
            .page_images
            .iter()
            .map(|bytes| {
                let image = rsf_page::PageImage::decode(bytes)
                    .map_err(|reason| RsfError::new(ErrorCode::InvalidInput, reason))?;
                Ok((image.id, bytes.clone()))
            })
            .collect::<RsfResult<_>>()?;
        self.pages = pages;
        self.table_pages = table_pages;
        self.row_pages = row_pages;
        self.page_wal_base_images = base_images;
        self.page_wal_redo_records = snapshot.redo_records;
        self.buffer = BufferPool::new(128).expect("valid buffer capacity");
        self.wal = WalGate::new(WalPolicy::new(1024 * 1024, true).expect("valid wal policy"));
        self.next_page_lsn = next_page_lsn;
        Ok(report)
    }

    fn apply_page_wal(
        &mut self,
        table: &str,
        operation: RedoOperation,
        commit_version: CommitVersion,
        byte_delta: i32,
        row_key: Vec<u8>,
        row_payload: Option<Vec<u8>>,
    ) -> RsfResult<Option<PageFrame>> {
        if operation == RedoOperation::SplitPage {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SQL page adapter requires row redo; use atomic relocation for page moves",
            ));
        }
        let decision = self.layout_decision_for_table(table)?;
        if decision.layout == SqlTableLayout::OrderedKv {
            return Ok(None);
        }
        if operation == RedoOperation::UpdateRow {
            let page = self.row_page(&row_key)?;
            let mut image = self
                .pages
                .read_image(&page, CommitVersion(0))
                .map_err(|reason| RsfError::new(ErrorCode::CorruptionDetected, reason))?;
            let payload = row_payload.as_ref().ok_or_else(|| {
                RsfError::new(ErrorCode::InvalidInput, "update requires row payload")
            })?;
            if image.update_row(row_key.clone(), payload.clone()).is_err() {
                return self
                    .move_page_row(
                        table,
                        commit_version,
                        row_key,
                        payload.clone(),
                        decision.page_size,
                    )
                    .map(Some);
            }
        }
        let page = if operation == RedoOperation::InsertRow {
            self.insertion_page(
                table,
                &row_key,
                row_payload.as_deref().ok_or_else(|| {
                    RsfError::new(ErrorCode::InvalidInput, "insert requires row payload")
                })?,
                decision.page_size,
                None,
            )?
        } else {
            self.row_page(&row_key)?
        };
        let index_key = row_key.clone();
        let deleted = operation == RedoOperation::DeleteRow;
        let current = self.read_page_frame(&page, CommitVersion(0))?;
        let lsn = self.allocate_page_lsn(commit_version)?;
        let redo_bytes = byte_delta.unsigned_abs().max(1);
        match self.wal.append(page.clone(), lsn, redo_bytes) {
            WalDecision::Appended(_) => {}
            WalDecision::Rejected { reason, .. } => {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("relational wal append rejected: {reason}"),
                ));
            }
            other => {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("unexpected relational wal append decision {other:?}"),
                ));
            }
        }
        match self.wal.sync() {
            WalDecision::Synced { .. } => {}
            WalDecision::Rejected { reason, .. } => {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("relational wal sync rejected: {reason}"),
                ));
            }
            other => {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("unexpected relational wal sync decision {other:?}"),
                ));
            }
        }

        expect_buffer_ok(self.buffer.pin(page.clone(), current.bytes), "pin")?;
        let redo = match operation {
            RedoOperation::InsertRow => RedoRecord::insert_row(
                page.clone(),
                current.lsn,
                lsn,
                row_key,
                row_payload.ok_or_else(|| {
                    RsfError::new(ErrorCode::InvalidInput, "insert redo requires row payload")
                })?,
            ),
            RedoOperation::UpdateRow => RedoRecord::update_row(
                page.clone(),
                current.lsn,
                lsn,
                row_key,
                row_payload.ok_or_else(|| {
                    RsfError::new(ErrorCode::InvalidInput, "update redo requires row payload")
                })?,
            ),
            RedoOperation::DeleteRow => {
                RedoRecord::delete_row(page.clone(), current.lsn, lsn, row_key)
            }
            RedoOperation::SplitPage => {
                unreachable!("split operations are rejected before WAL append")
            }
        };
        let encoded_redo = redo.encode().map_err(|reason| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("relational redo encoding rejected: {reason}"),
            )
        })?;
        let applied = match self.pages.apply_redo(redo) {
            PageDecision::Applied(frame) => frame,
            PageDecision::Rejected { reason, .. } => {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("relational page redo rejected: {reason}"),
                ));
            }
            other => {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("unexpected relational page redo decision {other:?}"),
                ));
            }
        };
        self.page_wal_redo_records.push(encoded_redo);
        expect_buffer_ok(self.buffer.mark_dirty_at(&page, applied.lsn), "dirty")?;
        expect_buffer_ok(
            self.buffer.flush_after_wal(
                FlushRequest::new(page.clone(), applied.lsn, applied.bytes),
                &self.wal,
            ),
            "flush",
        )?;
        expect_buffer_ok(self.buffer.unpin(&page), "unpin")?;
        if deleted {
            self.row_pages.remove(&index_key);
        } else {
            self.row_pages.insert(index_key, page);
        }
        Ok(Some(applied))
    }

    fn layout_decision_for_table(&self, table: &str) -> RsfResult<SqlLayoutDecision> {
        self.layout_decisions.get(table).cloned().ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("table {table} does not have a layout decision"),
            )
        })
    }

    fn read_page_frame(&self, page: &PageId, min_lsn: CommitVersion) -> RsfResult<PageFrame> {
        match self.pages.read_page(page, min_lsn) {
            PageDecision::Read(frame) => Ok(frame),
            PageDecision::Rejected { reason, .. } => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("relational page read rejected: {reason}"),
            )),
            other => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("unexpected relational page read decision {other:?}"),
            )),
        }
    }

    fn allocate_page_lsn(&mut self, commit_version: CommitVersion) -> RsfResult<CommitVersion> {
        let lsn = self.next_page_lsn.max(commit_version.0);
        self.next_page_lsn = lsn
            .checked_add(1)
            .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "SQL page LSN exhausted"))?;
        Ok(CommitVersion(lsn))
    }

    fn rebuild_index(&mut self, table: &str, descriptor: &IndexDescriptor) -> RsfResult<()> {
        let Some(table_record) = self.tables.get(table) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown table {table}"),
            ));
        };
        let planner = IndexPlanner::new(&table_record.descriptor, vec![descriptor.clone()])?;
        let rows = self
            .committed_rows_for_table(table)
            .into_iter()
            .filter(|row| row_has_index_values(row, descriptor))
            .map(|row| row_image(&row))
            .collect::<Vec<_>>();
        let rebuild = planner.plan_rebuild(descriptor, &rows)?;
        self.indexes.apply_rebuild(&rebuild);
        Ok(())
    }

    fn committed_rows_for_table(&self, table: &str) -> Vec<Row> {
        let Some(table_record) = self.tables.get(table) else {
            return Vec::new();
        };
        self.kv
            .scan_prefix(table_row_prefix(table).as_bytes())
            .into_iter()
            .filter_map(|(key, value)| {
                self.physical_row_from_value(table_record, &key, &value.value, value.version)
            })
            .collect()
    }

    fn physical_row(&self, table: &Table, primary_key: &str) -> Option<Row> {
        let value = self.kv.get(&logical_row_key(&table.name, primary_key))?;
        self.physical_row_from_value(
            table,
            &logical_row_key(&table.name, primary_key),
            &value.value,
            value.version,
        )
    }

    fn physical_primary_row(&self, table: &Table, primary_key: &str) -> Option<Row> {
        match self
            .primary
            .lookup(&table.primary_descriptor, &[primary_key.to_string()])
            .ok()?
        {
            PrimaryDecision::Found(_) => self.physical_row(table, primary_key),
            PrimaryDecision::Missing { .. }
            | PrimaryDecision::Inserted(_)
            | PrimaryDecision::Updated { .. }
            | PrimaryDecision::Deleted(_)
            | PrimaryDecision::Rejected { .. } => None,
        }
    }

    fn physical_row_from_value(
        &self,
        table: &Table,
        key: &[u8],
        bytes: &[u8],
        version: CommitVersion,
    ) -> Option<Row> {
        let primary_key = primary_key_from_logical_row_key(&table.name, key).ok()?;
        self.row_from_physical_bytes(table, &primary_key, bytes, version)
            .ok()
    }

    fn row_from_physical_bytes(
        &self,
        table: &Table,
        primary_key: &str,
        bytes: &[u8],
        version: CommitVersion,
    ) -> RsfResult<Row> {
        let decoded = RowCodec.decode(&table.descriptor, bytes)?;
        let mut columns = BTreeMap::new();
        for (column, value) in decoded.values {
            if let Some(value) = row_value_to_sql_string(value)? {
                columns.insert(column, value);
            }
        }
        Ok(Row {
            table: table.name.clone(),
            primary_key: primary_key.to_string(),
            columns,
            version,
        })
    }

    fn index_row(&mut self, row: &Row) -> RsfResult<()> {
        let Some(table) = self.tables.get(&row.table) else {
            return Ok(());
        };
        let descriptors = secondary_index_descriptors(table)
            .into_iter()
            .filter(|descriptor| row_has_index_values(row, descriptor))
            .collect::<Vec<_>>();
        if descriptors.is_empty() {
            return Ok(());
        }
        let plan = IndexPlanner::new(&table.descriptor, descriptors)?
            .plan_insert(&table.descriptor, &row_image(row))?;
        self.indexes.apply(&plan);
        Ok(())
    }

    fn deindex_row(&mut self, row: &Row) -> RsfResult<()> {
        let Some(table) = self.tables.get(&row.table) else {
            return Ok(());
        };
        let descriptors = secondary_index_descriptors(table)
            .into_iter()
            .filter(|descriptor| row_has_index_values(row, descriptor))
            .collect::<Vec<_>>();
        if descriptors.is_empty() {
            return Ok(());
        }
        let plan = IndexPlanner::new(&table.descriptor, descriptors)?
            .plan_delete(&table.descriptor, &row_image(row))?;
        self.indexes.apply(&plan);
        Ok(())
    }

    fn encode_row_payload(
        &self,
        table: &str,
        primary_key: &str,
        columns: &BTreeMap<String, String>,
        schema_version: CommitVersion,
    ) -> RsfResult<Vec<u8>> {
        let table_record = self.table_record(table)?;
        RowCodec
            .encode(
                &table_record.descriptor,
                typed_row_values(table_record, primary_key, columns)?,
                RowCodecPolicy {
                    schema_version,
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            )
            .map(|encoded| encoded.bytes)
    }

    fn row_from_tuple(
        &self,
        table: &Table,
        primary_key: &str,
        tuple: &TupleVersion,
    ) -> RsfResult<Row> {
        let decoded = RowCodec.decode(&table.descriptor, &tuple.row)?;
        let mut columns = BTreeMap::new();
        for (column, value) in decoded.values {
            if let Some(value) = row_value_to_sql_string(value)? {
                columns.insert(column, value);
            }
        }
        Ok(Row {
            table: table.name.clone(),
            primary_key: primary_key.to_string(),
            columns,
            version: tuple.header.create_version,
        })
    }

    fn apply_tuple_insert(
        &mut self,
        table: &str,
        primary_key: &str,
        row_payload: Vec<u8>,
        version: CommitVersion,
    ) -> RsfResult<()> {
        match self
            .tuples
            .insert(tuple_key(table, primary_key), row_payload, version, version)?
        {
            TupleDecision::Inserted(_) => Ok(()),
            TupleDecision::Rejected { reason, .. } => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("tuple insert rejected: {reason}"),
            )),
            other => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("unexpected tuple insert decision {other:?}"),
            )),
        }
    }

    fn apply_tuple_update(
        &mut self,
        table: &str,
        primary_key: &str,
        row_payload: Vec<u8>,
        version: CommitVersion,
    ) -> RsfResult<()> {
        match self.tuples.update(
            &tuple_key(table, primary_key),
            row_payload,
            version,
            version,
        )? {
            TupleDecision::Updated { .. } => Ok(()),
            TupleDecision::Rejected { reason, .. } => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("tuple update rejected: {reason}"),
            )),
            other => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("unexpected tuple update decision {other:?}"),
            )),
        }
    }

    fn apply_tuple_delete(
        &mut self,
        table: &str,
        primary_key: &str,
        version: CommitVersion,
    ) -> RsfResult<()> {
        match self
            .tuples
            .delete(&tuple_key(table, primary_key), version)?
        {
            TupleDecision::Deleted(_) => Ok(()),
            TupleDecision::Rejected { reason, .. } => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("tuple delete rejected: {reason}"),
            )),
            other => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("unexpected tuple delete decision {other:?}"),
            )),
        }
    }

    fn apply_primary_insert(
        &mut self,
        table: &str,
        primary_key: &str,
        columns: &BTreeMap<String, String>,
        pg: PlacementGroupId,
        version: CommitVersion,
    ) -> RsfResult<()> {
        let table_record = self.table_record(table)?;
        let row = primary_row(table_record, primary_key, columns, pg, version);
        let descriptor = table_record.primary_descriptor.clone();
        match self.primary.insert(&descriptor, row)? {
            PrimaryDecision::Inserted(_) => Ok(()),
            PrimaryDecision::Rejected { reason, .. } => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("primary index insert rejected: {reason}"),
            )),
            other => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("unexpected primary index insert decision {other:?}"),
            )),
        }
    }

    fn apply_primary_update(
        &mut self,
        table: &str,
        primary_key: &str,
        columns: &BTreeMap<String, String>,
        pg: PlacementGroupId,
        version: CommitVersion,
    ) -> RsfResult<()> {
        let table_record = self.table_record(table)?;
        let row = primary_row(table_record, primary_key, columns, pg, version);
        let descriptor = table_record.primary_descriptor.clone();
        match self
            .primary
            .update(&descriptor, &[primary_key.to_string()], row)?
        {
            PrimaryDecision::Updated { .. } => Ok(()),
            PrimaryDecision::Missing { .. } => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("primary key {primary_key} is missing from primary index"),
            )),
            PrimaryDecision::Rejected { reason, .. } => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("primary index update rejected: {reason}"),
            )),
            other => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("unexpected primary index update decision {other:?}"),
            )),
        }
    }

    fn apply_primary_delete(&mut self, table: &str, primary_key: &str) -> RsfResult<()> {
        let table_record = self.table_record(table)?;
        let descriptor = table_record.primary_descriptor.clone();
        match self
            .primary
            .delete(&descriptor, &[primary_key.to_string()])?
        {
            PrimaryDecision::Deleted(_) => Ok(()),
            PrimaryDecision::Missing { .. } => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("primary key {primary_key} is missing from primary index"),
            )),
            other => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("unexpected primary index delete decision {other:?}"),
            )),
        }
    }

    fn validate_constraints_for_values(
        &self,
        table: &str,
        primary_key: &str,
        columns: &BTreeMap<String, String>,
    ) -> RsfResult<()> {
        let table_record = self.table_record(table)?;
        validate_row_constraints(
            table_record,
            &constraint_mutation(table_record, primary_key, columns),
            &self.constraints,
        )
    }

    fn validate_constraints_for_update(
        &self,
        table: &str,
        primary_key: &str,
        existing: &Row,
        columns: &BTreeMap<String, String>,
    ) -> RsfResult<()> {
        let table_record = self.table_record(table)?;
        let mut state = self.constraints.clone();
        remove_constraint_state_for(&mut state, table_record, existing);
        validate_row_constraints(
            table_record,
            &constraint_mutation(table_record, primary_key, columns),
            &state,
        )
    }

    fn validate_existing_unique_constraint(
        &self,
        table: &Table,
        rule: &ConstraintRule,
    ) -> RsfResult<()> {
        let ConstraintRule::Unique { name, columns } = rule else {
            return Ok(());
        };
        let mut seen = BTreeSet::new();
        for row in self.rows.values().filter(|row| row.table == table.name) {
            let Some(key) = optional_constraint_key(table, row, columns) else {
                continue;
            };
            if !seen.insert(key.clone()) {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    format!(
                        "existing rows violate unique constraint {name}: {}",
                        key.join("/")
                    ),
                ));
            }
        }
        Ok(())
    }

    fn validate_existing_foreign_key_constraint(
        &self,
        table: &Table,
        rule: &ConstraintRule,
    ) -> RsfResult<()> {
        let ConstraintRule::ForeignKey { name, columns, .. } = rule else {
            return Ok(());
        };
        let validator = ConstraintValidator::new(&table.descriptor, vec![rule.clone()])?;
        for row in self.rows.values().filter(|row| row.table == table.name) {
            let mutation = constraint_mutation(table, &row.primary_key, &row.columns);
            let decision = validator.validate(&table.descriptor, &mutation, &self.constraints)?;
            if let ConstraintDecision::Rejected { violations, .. } = decision {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    format!(
                        "existing row {} violates foreign key {name} on {}: {}",
                        row.primary_key,
                        columns.join(","),
                        violations
                            .into_iter()
                            .map(|violation| violation.detail)
                            .collect::<Vec<_>>()
                            .join("; ")
                    ),
                ));
            }
        }
        Ok(())
    }

    fn add_constraint_state_for_row(&mut self, table: &str, row: &Row) -> RsfResult<()> {
        let table_record = self.table_record(table)?.clone();
        add_constraint_state_for(&mut self.constraints, &table_record, row);
        Ok(())
    }

    fn remove_constraint_state_for_row(&mut self, table: &str, row: &Row) -> RsfResult<()> {
        let table_record = self.table_record(table)?.clone();
        remove_constraint_state_for(&mut self.constraints, &table_record, row);
        Ok(())
    }

    fn register_recovered_table(&mut self, recovered: SqlRecoveredTable) -> RsfResult<()> {
        let table_name = recovered.descriptor.name.0.clone();
        self.register_table_descriptor_with_profile(recovered.descriptor, recovered.profile)?;
        for constraint in recovered.constraints {
            match constraint {
                ConstraintRule::PrimaryKey { .. } => {}
                ConstraintRule::Unique { name, columns } => {
                    self.add_unique_constraint(&table_name, name, columns)?;
                }
                ConstraintRule::ForeignKey {
                    name,
                    columns,
                    referenced_table,
                    referenced_columns,
                } => {
                    self.add_foreign_key_constraint(
                        &table_name,
                        name,
                        columns,
                        referenced_table.0,
                        referenced_columns,
                    )?;
                }
                ConstraintRule::NotNull { column } => {
                    self.tables
                        .get_mut(&table_name)
                        .expect("table was registered")
                        .constraints
                        .push(ConstraintRule::NotNull { column });
                }
            }
        }
        for column in recovered.secondary_indexes {
            self.create_index(&table_name, &column)?;
        }
        Ok(())
    }

    fn replay_recovered_tuple_versions(
        &mut self,
        table: &Table,
        primary_key: &str,
        versions: &[KvVersionedValue],
        row_versions: &mut usize,
        tombstones: &mut usize,
        highest_version: &mut CommitVersion,
    ) -> RsfResult<Option<Row>> {
        if versions.is_empty() {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "recovered row {}/{} has no versions",
                    table.name, primary_key
                ),
            ));
        }
        let mut last_version = CommitVersion(0);
        let mut tuple_live = false;
        let mut latest_row = None;

        for value in versions {
            if value.version <= last_version {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "recovered row {}/{} versions are not strictly increasing",
                        table.name, primary_key
                    ),
                ));
            }
            last_version = value.version;
            *highest_version = (*highest_version).max(value.version);
            if value.deleted {
                *tombstones += 1;
                if tuple_live {
                    self.apply_tuple_delete(&table.name, primary_key, value.version)?;
                    tuple_live = false;
                }
                latest_row = None;
                continue;
            }

            *row_versions += 1;
            let row =
                self.row_from_physical_bytes(table, primary_key, &value.value, value.version)?;
            let row_payload =
                self.encode_row_payload(&table.name, primary_key, &row.columns, value.version)?;
            if tuple_live {
                self.apply_tuple_update(&table.name, primary_key, row_payload, value.version)?;
            } else {
                self.apply_tuple_insert(&table.name, primary_key, row_payload, value.version)?;
                tuple_live = true;
            }
            latest_row = Some(row);
        }

        Ok(latest_row)
    }

    fn validate_recovered_row(&self, table: &Table, row: &Row) -> RsfResult<()> {
        self.validate_row_columns(&table.name, &row.columns)?;
        validate_row_constraints(
            table,
            &constraint_mutation(table, &row.primary_key, &row.columns),
            &self.constraints,
        )
        .map_err(|err| {
            RsfError::new(
                err.code,
                format!(
                    "recovered row {}.{} failed validation: {}",
                    row.table, row.primary_key, err.message
                ),
            )
        })
    }

    fn table_record(&self, table: &str) -> RsfResult<&Table> {
        self.tables
            .get(table)
            .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, format!("unknown table {table}")))
    }

    fn sql_request(&self, label: impl Into<String>) -> ClientRequestId {
        ClientRequestId::new(
            self.request_client
                .clone()
                .unwrap_or_else(|| format!("sql:{}", label.into())),
            0,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlWriteResult {
    pub row: Row,
    pub evidence: CommitEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ParsedStatement {
    CreateTable {
        table: String,
        primary_key: String,
        columns: Vec<String>,
    },
    CreateIndex {
        table: String,
        column: String,
    },
    Insert {
        table: String,
        columns: BTreeMap<String, String>,
    },
    Update {
        table: String,
        predicate: EqualityPredicate,
        assignments: BTreeMap<String, String>,
    },
    Delete {
        table: String,
        predicate: EqualityPredicate,
    },
    Select {
        table: String,
        projection: Projection,
        predicate: Option<Predicate>,
        limit: Option<usize>,
    },
    Begin,
    Commit,
    Rollback,
    Savepoint {
        name: String,
    },
    RollbackToSavepoint {
        name: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EqualityPredicate {
    column: String,
    value: String,
}

/// Classifies a statement using the execution parser, for authorization before
/// opening a namespace's durable SQL state.
pub fn statement_is_read_only(statement: &str) -> RsfResult<bool> {
    Ok(matches!(
        parse_statement(statement)?,
        ParsedStatement::Select { .. }
    ))
}

fn parse_statement(statement: &str) -> RsfResult<ParsedStatement> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let uppercase = trimmed.to_ascii_uppercase();
    if uppercase.starts_with("CREATE TABLE ") {
        parse_create_table(trimmed)
    } else if uppercase == "BEGIN" || uppercase == "BEGIN TRANSACTION" {
        Ok(ParsedStatement::Begin)
    } else if uppercase == "COMMIT" {
        Ok(ParsedStatement::Commit)
    } else if uppercase == "ROLLBACK" {
        Ok(ParsedStatement::Rollback)
    } else if uppercase.starts_with("SAVEPOINT ") {
        parse_savepoint(trimmed)
    } else if uppercase.starts_with("ROLLBACK TO SAVEPOINT ")
        || uppercase.starts_with("ROLLBACK TO ")
    {
        parse_rollback_to_savepoint(trimmed)
    } else if uppercase.starts_with("CREATE INDEX ") {
        parse_create_index(trimmed)
    } else if uppercase.starts_with("INSERT INTO ") {
        parse_insert(trimmed)
    } else if uppercase.starts_with("UPDATE ") {
        parse_update(trimmed)
    } else if uppercase.starts_with("DELETE FROM ") {
        parse_delete(trimmed)
    } else if uppercase.starts_with("SELECT ") {
        parse_select(trimmed)
    } else {
        Err(RsfError::new(
            ErrorCode::InvalidInput,
            "unsupported SQL statement",
        ))
    }
}

fn parse_savepoint(statement: &str) -> RsfResult<ParsedStatement> {
    let name = statement["SAVEPOINT ".len()..].trim();
    validate_savepoint_name(name)?;
    Ok(ParsedStatement::Savepoint {
        name: name.to_string(),
    })
}

fn parse_rollback_to_savepoint(statement: &str) -> RsfResult<ParsedStatement> {
    let upper = statement.to_ascii_uppercase();
    let name = if upper.starts_with("ROLLBACK TO SAVEPOINT ") {
        statement["ROLLBACK TO SAVEPOINT ".len()..].trim()
    } else {
        statement["ROLLBACK TO ".len()..].trim()
    };
    validate_savepoint_name(name)?;
    Ok(ParsedStatement::RollbackToSavepoint {
        name: name.to_string(),
    })
}

fn parse_create_table(statement: &str) -> RsfResult<ParsedStatement> {
    let rest = statement
        .strip_prefix("CREATE TABLE ")
        .or_else(|| statement.strip_prefix("create table "))
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "invalid CREATE TABLE"))?;
    let open = rest
        .find('(')
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "CREATE TABLE missing columns"))?;
    let close = rest
        .rfind(')')
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "CREATE TABLE missing ')'"))?;
    let table = rest[..open].trim().to_string();
    let columns = rest[open + 1..close]
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>();
    let declared_columns = columns
        .iter()
        .map(|column| {
            column
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_string()
        })
        .collect::<Vec<_>>();
    let unique_columns = declared_columns.iter().cloned().collect::<BTreeSet<_>>();
    if unique_columns.len() != declared_columns.len() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "CREATE TABLE contains duplicate columns",
        ));
    }
    let primary_key = columns
        .iter()
        .find_map(|column| {
            let mut parts = column.split_whitespace();
            let name = parts.next()?;
            let tail = parts.collect::<Vec<_>>().join(" ").to_ascii_uppercase();
            tail.contains("PRIMARY KEY").then(|| name.to_string())
        })
        .ok_or_else(|| {
            RsfError::new(ErrorCode::InvalidInput, "CREATE TABLE missing primary key")
        })?;

    if table.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "CREATE TABLE missing table name",
        ));
    }

    validate_declared_columns(&table, &primary_key, &unique_columns)?;

    Ok(ParsedStatement::CreateTable {
        table,
        primary_key,
        columns: declared_columns,
    })
}

fn parse_create_index(statement: &str) -> RsfResult<ParsedStatement> {
    let rest = statement
        .strip_prefix("CREATE INDEX ")
        .or_else(|| statement.strip_prefix("create index "))
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "invalid CREATE INDEX"))?;
    let uppercase = rest.to_ascii_uppercase();
    let on_pos = uppercase
        .find(" ON ")
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "CREATE INDEX missing ON"))?;
    let target = rest[on_pos + " ON ".len()..].trim();
    let open = target
        .find('(')
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "CREATE INDEX missing column"))?;
    let close = target
        .rfind(')')
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "CREATE INDEX missing ')'"))?;
    Ok(ParsedStatement::CreateIndex {
        table: target[..open].trim().to_string(),
        column: target[open + 1..close].trim().to_string(),
    })
}

fn parse_insert(statement: &str) -> RsfResult<ParsedStatement> {
    let rest = statement
        .strip_prefix("INSERT INTO ")
        .or_else(|| statement.strip_prefix("insert into "))
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "invalid INSERT"))?;
    let values_pos = rest
        .to_ascii_uppercase()
        .find(" VALUES ")
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "INSERT missing VALUES"))?;
    let target = rest[..values_pos].trim();
    let values = rest[values_pos + " VALUES ".len()..].trim();

    let target_open = target
        .find('(')
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "INSERT missing column list"))?;
    let target_close = target
        .rfind(')')
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "INSERT column list missing ')'"))?;
    let table = target[..target_open].trim().to_string();
    let columns = target[target_open + 1..target_close]
        .split(',')
        .map(|column| column.trim().to_string())
        .collect::<Vec<_>>();

    let values_open = values
        .find('(')
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "INSERT missing values list"))?;
    let values_close = values
        .rfind(')')
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "INSERT values list missing ')'"))?;
    let values = values[values_open + 1..values_close]
        .split(',')
        .map(parse_literal)
        .collect::<Vec<_>>();

    if columns.len() != values.len() || columns.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "INSERT columns and values must match",
        ));
    }

    Ok(ParsedStatement::Insert {
        table,
        columns: columns.into_iter().zip(values).collect::<BTreeMap<_, _>>(),
    })
}

fn parse_update(statement: &str) -> RsfResult<ParsedStatement> {
    let rest = statement
        .strip_prefix("UPDATE ")
        .or_else(|| statement.strip_prefix("update "))
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "invalid UPDATE"))?;
    let uppercase = rest.to_ascii_uppercase();
    let set_pos = uppercase
        .find(" SET ")
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "UPDATE missing SET"))?;
    let where_pos = uppercase
        .find(" WHERE ")
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "UPDATE missing WHERE"))?;
    let table = rest[..set_pos].trim().to_string();
    let assignments = parse_assignments(&rest[set_pos + " SET ".len()..where_pos])?;
    let predicate = parse_equality_predicate(&rest[where_pos + " WHERE ".len()..])?;
    Ok(ParsedStatement::Update {
        table,
        predicate,
        assignments,
    })
}

fn parse_delete(statement: &str) -> RsfResult<ParsedStatement> {
    let rest = statement
        .strip_prefix("DELETE FROM ")
        .or_else(|| statement.strip_prefix("delete from "))
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "invalid DELETE"))?;
    let uppercase = rest.to_ascii_uppercase();
    let where_pos = uppercase
        .find(" WHERE ")
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "DELETE missing WHERE"))?;
    let table = rest[..where_pos].trim().to_string();
    let predicate = parse_equality_predicate(&rest[where_pos + " WHERE ".len()..])?;
    Ok(ParsedStatement::Delete { table, predicate })
}

fn parse_select(statement: &str) -> RsfResult<ParsedStatement> {
    let uppercase = statement.to_ascii_uppercase();
    let from_pos = uppercase
        .find(" FROM ")
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "SELECT missing FROM"))?;
    let projection = parse_projection(&statement["SELECT ".len()..from_pos])?;
    let mut tail = statement[from_pos + " FROM ".len()..].trim();
    let tail_upper = tail.to_ascii_uppercase();
    let where_pos = tail_upper.find(" WHERE ");
    let limit_pos = tail_upper.find(" LIMIT ");
    let table_end = where_pos.or(limit_pos).unwrap_or(tail.len());
    let table = tail[..table_end].trim().to_string();
    if table.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "SELECT missing table",
        ));
    }
    tail = tail[table_end..].trim();

    let mut predicate = None;
    let mut limit = None;
    if tail.to_ascii_uppercase().starts_with("WHERE ") {
        let predicate_tail = &tail["WHERE ".len()..];
        let predicate_upper = predicate_tail.to_ascii_uppercase();
        let predicate_end = predicate_upper
            .find(" LIMIT ")
            .unwrap_or(predicate_tail.len());
        predicate = Some(parse_select_predicate(&predicate_tail[..predicate_end])?);
        tail = predicate_tail[predicate_end..].trim();
    }
    if tail.to_ascii_uppercase().starts_with("LIMIT ") {
        let value = tail["LIMIT ".len()..].trim();
        if value.is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SELECT LIMIT missing value",
            ));
        }
        limit = Some(value.parse::<usize>().map_err(|_| {
            RsfError::new(ErrorCode::InvalidInput, "SELECT LIMIT must be an integer")
        })?);
    } else if !tail.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "unsupported SELECT clause",
        ));
    }

    Ok(ParsedStatement::Select {
        table,
        projection,
        predicate,
        limit,
    })
}

fn parse_projection(input: &str) -> RsfResult<Projection> {
    let input = input.trim();
    if input == "*" {
        return Ok(Projection::All);
    }
    let columns = input
        .split(',')
        .map(|column| column.trim().to_string())
        .collect::<Vec<_>>();
    if columns.is_empty() || columns.iter().any(|column| column.is_empty()) {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "SELECT projection must include columns",
        ));
    }
    Ok(Projection::Columns(columns))
}

fn parse_select_predicate(input: &str) -> RsfResult<Predicate> {
    let Some((column, value)) = input.trim().split_once('=') else {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "SELECT predicate must be equality",
        ));
    };
    let column = column.trim().to_string();
    if column.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "SELECT predicate column must not be empty",
        ));
    }
    Ok(Predicate::Eq {
        column,
        value: QueryValue::Text(parse_literal(value)),
    })
}

fn parse_equality_predicate(predicate: &str) -> RsfResult<EqualityPredicate> {
    let Some((column, value)) = predicate.trim().split_once('=') else {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "predicate must be equality",
        ));
    };
    let column = column.trim().to_string();
    if column.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "predicate column must not be empty",
        ));
    }
    Ok(EqualityPredicate {
        column,
        value: parse_literal(value),
    })
}

fn parse_assignments(assignments: &str) -> RsfResult<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for assignment in assignments.split(',') {
        let Some((column, value)) = assignment.split_once('=') else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "assignment must use equality",
            ));
        };
        parsed.insert(column.trim().to_string(), parse_literal(value));
    }
    if parsed.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "missing assignments",
        ));
    }
    Ok(parsed)
}

fn parse_literal(value: &str) -> String {
    value
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string()
}

fn table_from_descriptor(descriptor: TableDescriptor) -> RsfResult<Table> {
    if descriptor.primary_key.len() != 1 {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "SQL surface currently requires exactly one primary key column",
        ));
    }
    let name = descriptor.name.0.clone();
    let primary_key = descriptor
        .primary_key
        .first()
        .expect("length checked")
        .clone();
    let columns = descriptor
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<BTreeSet<_>>();
    if columns.len() != descriptor.columns.len() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!("table {} contains duplicate columns", descriptor.name.0),
        ));
    }
    validate_declared_columns(&name, &primary_key, &columns)?;
    let primary_descriptor = PrimaryKeyDescriptor::from_table(&descriptor)?;
    let constraints = vec![ConstraintRule::PrimaryKey {
        columns: descriptor.primary_key.clone(),
    }];
    Ok(Table {
        name,
        primary_key,
        columns,
        descriptor,
        primary_descriptor,
        constraints,
        secondary_indexes: Vec::new(),
        layout_profile: SqlLayoutProfile::Hybrid,
        layout: SqlTableLayout::PageWal,
        page_size: DEFAULT_PAGE_SIZE,
    })
}

fn choose_table_layout(table: &Table, profile: SqlLayoutProfile) -> SqlLayoutDecision {
    match profile {
        SqlLayoutProfile::OrderedKv => SqlLayoutDecision {
            table: table.name.clone(),
            profile,
            layout: SqlTableLayout::OrderedKv,
            page_size: 0,
            reason: "profile requested ordered-kv layout".to_string(),
        },
        SqlLayoutProfile::PageWal => SqlLayoutDecision {
            table: table.name.clone(),
            profile,
            layout: SqlTableLayout::PageWal,
            page_size: DEFAULT_PAGE_SIZE,
            reason: "profile requested page-wal layout".to_string(),
        },
        SqlLayoutProfile::Hybrid => SqlLayoutDecision {
            table: table.name.clone(),
            profile,
            layout: SqlTableLayout::PageWal,
            page_size: DEFAULT_PAGE_SIZE,
            reason: "hybrid profile uses page-wal primary layout with ordered-kv projection"
                .to_string(),
        },
    }
}

fn validate_declared_columns(
    table: &str,
    primary_key: &str,
    columns: &BTreeSet<String>,
) -> RsfResult<()> {
    if table.trim().is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "table name must not be empty",
        ));
    }
    if primary_key.trim().is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "primary key column must not be empty",
        ));
    }
    if columns.is_empty() || columns.iter().any(|column| column.trim().is_empty()) {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "table must declare non-empty columns",
        ));
    }
    if !columns.contains(primary_key) {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!("primary key column {primary_key} is not declared on table {table}"),
        ));
    }
    Ok(())
}

fn ensure_declared_column(table: &Table, column: &str) -> RsfResult<()> {
    if table.columns.contains(column) {
        return Ok(());
    }
    Err(RsfError::new(
        ErrorCode::InvalidInput,
        format!("column {column} is not declared on table {}", table.name),
    ))
}

fn projection_with_primary(table: &Table, projection: Projection) -> Projection {
    match projection {
        Projection::All => Projection::All,
        Projection::Columns(mut columns) => {
            if !columns.iter().any(|column| column == &table.primary_key) {
                columns.push(table.primary_key.clone());
            }
            Projection::Columns(columns)
        }
    }
}

fn apply_requested_projection(rows: &mut [Row], projection: &Projection) -> RsfResult<()> {
    let Projection::Columns(columns) = projection else {
        return Ok(());
    };
    for row in rows {
        let mut projected = BTreeMap::new();
        for column in columns {
            let value = row.columns.get(column).cloned().ok_or_else(|| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("row is missing projected column {column}"),
                )
            })?;
            projected.insert(column.clone(), value);
        }
        row.columns = projected;
    }
    Ok(())
}

fn query_row_from_row(table: &Table, row: &Row) -> RsfResult<QueryRow> {
    let mut columns = BTreeMap::new();
    for descriptor in &table.descriptor.columns {
        let Some(value) = row.columns.get(&descriptor.name) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "row {}.{} is missing column {}",
                    row.table, row.primary_key, descriptor.name
                ),
            ));
        };
        columns.insert(
            descriptor.name.clone(),
            query_value_from_sql_string(&descriptor.ty, value, &descriptor.name)?,
        );
    }
    Ok(QueryRow {
        table: table.descriptor.name.clone(),
        columns,
    })
}

fn row_from_query_row(table: &Table, row: QueryRow) -> RsfResult<Row> {
    let primary = row
        .columns
        .get(&table.primary_key)
        .ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("query row is missing primary key {}", table.primary_key),
            )
        })?
        .clone();
    let primary_key = query_value_to_sql_string(primary)?;
    let mut columns = BTreeMap::new();
    for (column, value) in row.columns {
        columns.insert(column, query_value_to_sql_string(value)?);
    }
    Ok(Row {
        table: table.name.clone(),
        primary_key,
        columns,
        version: CommitVersion(0),
    })
}

fn equality_text_value(predicate: Option<&Predicate>, column: &str) -> Option<String> {
    match predicate? {
        Predicate::Eq { column: key, value } if key == column => {
            query_value_to_sql_string(value.clone()).ok()
        }
        Predicate::Eq { .. } => None,
        Predicate::And(predicates) => predicates
            .iter()
            .find_map(|predicate| equality_text_value(Some(predicate), column)),
    }
}

fn type_select_predicate(
    table: &Table,
    predicate: Option<Predicate>,
) -> RsfResult<Option<Predicate>> {
    predicate
        .map(|predicate| type_predicate(table, predicate))
        .transpose()
}

fn type_predicate(table: &Table, predicate: Predicate) -> RsfResult<Predicate> {
    match predicate {
        Predicate::Eq { column, value } => {
            let descriptor = table
                .descriptor
                .columns
                .iter()
                .find(|descriptor| descriptor.name == column)
                .ok_or_else(|| {
                    RsfError::new(
                        ErrorCode::InvalidInput,
                        format!("unknown predicate column {column}"),
                    )
                })?;
            let value = match value {
                QueryValue::Text(value) => {
                    query_value_from_sql_string(&descriptor.ty, &value, &descriptor.name)?
                }
                value => value,
            };
            Ok(Predicate::Eq { column, value })
        }
        Predicate::And(predicates) => predicates
            .into_iter()
            .map(|predicate| type_predicate(table, predicate))
            .collect::<RsfResult<Vec<_>>>()
            .map(Predicate::And),
    }
}

fn query_value_from_sql_string(
    ty: &ColumnType,
    value: &str,
    column: &str,
) -> RsfResult<QueryValue> {
    match ty {
        ColumnType::Text => Ok(QueryValue::Text(value.to_string())),
        ColumnType::Int64 => value.parse::<i64>().map(QueryValue::Int64).map_err(|_| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("column {column} expected int64, got {value}"),
            )
        }),
        ColumnType::Bool => match value.to_ascii_lowercase().as_str() {
            "true" => Ok(QueryValue::Bool(true)),
            "false" => Ok(QueryValue::Bool(false)),
            _ => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("column {column} expected bool, got {value}"),
            )),
        },
        ColumnType::Bytes => Ok(QueryValue::Bytes(value.as_bytes().to_vec())),
    }
}

fn query_value_to_sql_string(value: QueryValue) -> RsfResult<String> {
    match value {
        QueryValue::Int64(value) => Ok(value.to_string()),
        QueryValue::Text(value) => Ok(value),
        QueryValue::Bool(value) => Ok(value.to_string()),
        QueryValue::Bytes(value) => String::from_utf8(value).map_err(|_| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "bytes query value cannot be represented as SQL text",
            )
        }),
    }
}

fn typed_row_values(
    table: &Table,
    primary_key: &str,
    columns: &BTreeMap<String, String>,
) -> RsfResult<BTreeMap<String, RowValue>> {
    let mut values = BTreeMap::new();
    for descriptor in &table.descriptor.columns {
        if let Some(value) = columns.get(&descriptor.name) {
            values.insert(
                descriptor.name.clone(),
                parse_row_value(&descriptor.ty, value, &descriptor.name)?,
            );
        }
    }
    if !values.contains_key(&table.primary_key) {
        let Some(primary_key_descriptor) = table
            .descriptor
            .columns
            .iter()
            .find(|column| column.name == table.primary_key)
        else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "primary key column {} is not declared on table {}",
                    table.primary_key, table.name
                ),
            ));
        };
        values.insert(
            table.primary_key.clone(),
            parse_row_value(
                &primary_key_descriptor.ty,
                primary_key,
                &primary_key_descriptor.name,
            )?,
        );
    }
    Ok(values)
}

fn parse_row_value(ty: &ColumnType, value: &str, column: &str) -> RsfResult<RowValue> {
    match ty {
        ColumnType::Text => Ok(RowValue::Text(value.to_string())),
        ColumnType::Int64 => value.parse::<i64>().map(RowValue::Int64).map_err(|_| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("column {column} expected int64, got {value}"),
            )
        }),
        ColumnType::Bool => match value.to_ascii_lowercase().as_str() {
            "true" => Ok(RowValue::Bool(true)),
            "false" => Ok(RowValue::Bool(false)),
            _ => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("column {column} expected bool, got {value}"),
            )),
        },
        ColumnType::Bytes => Ok(RowValue::Bytes(value.as_bytes().to_vec())),
    }
}

fn row_value_to_sql_string(value: RowValue) -> RsfResult<Option<String>> {
    match value {
        RowValue::Null => Ok(None),
        RowValue::Int64(value) => Ok(Some(value.to_string())),
        RowValue::Text(value) => Ok(Some(value)),
        RowValue::Bool(value) => Ok(Some(value.to_string())),
        RowValue::Bytes(value) => String::from_utf8(value).map(Some).map_err(|_| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "bytes column cannot be represented as SQL text",
            )
        }),
    }
}

fn tuple_key(table: &str, primary_key: &str) -> TupleKey {
    TupleKey::new(table, primary_key.as_bytes().to_vec())
}

fn secondary_index_descriptors(table: &Table) -> Vec<IndexDescriptor> {
    table
        .secondary_indexes
        .iter()
        .map(|column| secondary_index_descriptor(table, column))
        .collect()
}

fn secondary_index_descriptor(table: &Table, column: &str) -> IndexDescriptor {
    IndexDescriptor {
        table: table.descriptor.name.clone(),
        name: secondary_index_name(&table.name, column),
        columns: vec![column.to_string()],
        unique: false,
    }
}

fn secondary_index_name(table: &str, column: &str) -> String {
    format!("{table}_{column}")
}

fn row_image(row: &Row) -> RowImage {
    RowImage {
        primary_key: row.primary_key.clone(),
        values: row.columns.clone(),
    }
}

fn row_has_index_values(row: &Row, descriptor: &IndexDescriptor) -> bool {
    descriptor
        .columns
        .iter()
        .all(|column| row.columns.contains_key(column))
}

fn validate_row_constraints(
    table: &Table,
    mutation: &RowMutation,
    state: &ConstraintState,
) -> RsfResult<()> {
    let decision = ConstraintValidator::new(&table.descriptor, table.constraints.clone())?
        .validate(&table.descriptor, mutation, state)?;
    match decision {
        ConstraintDecision::Accepted { .. } => Ok(()),
        ConstraintDecision::Rejected { violations, .. } => Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!(
                "constraint violation: {}",
                violations
                    .into_iter()
                    .map(|violation| format!("{} ({})", violation.rule, violation.detail))
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        )),
    }
}

fn constraint_mutation(
    table: &Table,
    primary_key: &str,
    columns: &BTreeMap<String, String>,
) -> RowMutation {
    let mut values = BTreeMap::new();
    for column in &table.descriptor.columns {
        let value = if let Some(value) = columns.get(&column.name) {
            ConstraintValue::Text(value.clone())
        } else if column.name == table.primary_key {
            ConstraintValue::Text(primary_key.to_string())
        } else {
            ConstraintValue::Null
        };
        values.insert(column.name.clone(), value);
    }
    RowMutation {
        table: table.descriptor.name.clone(),
        values,
    }
}

fn add_constraint_state_for(state: &mut ConstraintState, table: &Table, row: &Row) {
    if let Some(primary_key) = concrete_constraint_key(table, row, &table.descriptor.primary_key) {
        state.add_referenced_key(
            &table.descriptor.name,
            &table.descriptor.primary_key,
            primary_key,
        );
    }
    for rule in &table.constraints {
        if let ConstraintRule::Unique { name, columns } = rule
            && let Some(key) = concrete_constraint_key(table, row, columns)
        {
            state.add_unique_key(name, key);
        }
    }
}

fn add_unique_constraint_state_for(state: &mut ConstraintState, table: &Table, row: &Row) {
    for rule in &table.constraints {
        if let ConstraintRule::Unique { name, columns } = rule
            && let Some(key) = concrete_constraint_key(table, row, columns)
        {
            state.add_unique_key(name, key);
        }
    }
}

fn remove_constraint_state_for(state: &mut ConstraintState, table: &Table, row: &Row) {
    if let Some(primary_key) = concrete_constraint_key(table, row, &table.descriptor.primary_key) {
        state.remove_referenced_key(
            &table.descriptor.name,
            &table.descriptor.primary_key,
            primary_key,
        );
    }
    for rule in &table.constraints {
        if let ConstraintRule::Unique { name, columns } = rule
            && let Some(key) = concrete_constraint_key(table, row, columns)
        {
            state.remove_unique_key(name, key);
        }
    }
}

fn optional_constraint_key(table: &Table, row: &Row, columns: &[String]) -> Option<Vec<String>> {
    concrete_constraint_key(table, row, columns)
}

fn concrete_constraint_key(table: &Table, row: &Row, columns: &[String]) -> Option<Vec<String>> {
    let mut key = Vec::new();
    for column in columns {
        let value = if column == &table.primary_key && !row.columns.contains_key(column) {
            Some(&row.primary_key)
        } else {
            row.columns.get(column)
        }?;
        if value.is_empty() {
            return None;
        }
        key.push(value.clone());
    }
    Some(key)
}

fn primary_row(
    table: &Table,
    primary_key: &str,
    columns: &BTreeMap<String, String>,
    placement_group: PlacementGroupId,
    version: CommitVersion,
) -> PrimaryRow {
    let mut values = columns.clone();
    values
        .entry(table.primary_key.clone())
        .or_insert_with(|| primary_key.to_string());
    PrimaryRow {
        values,
        row_key: logical_row_key(&table.name, primary_key),
        placement_group,
        version,
    }
}

fn logical_row_key(table: &str, primary_key: &str) -> Vec<u8> {
    format!("table/{table}/pk/{primary_key}").into_bytes()
}

fn table_row_prefix(table: &str) -> String {
    format!("table/{table}/pk/")
}

fn primary_key_from_logical_row_key(table: &str, key: &[u8]) -> RsfResult<String> {
    let prefix = table_row_prefix(table);
    let key = std::str::from_utf8(key).map_err(|_| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("physical row key for table {table} is not utf-8"),
        )
    })?;
    key.strip_prefix(&prefix)
        .filter(|primary_key| !primary_key.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("physical row key {key} does not match table {table}"),
            )
        })
}

fn expect_buffer_ok(decision: BufferDecision, action: &str) -> RsfResult<()> {
    match decision {
        BufferDecision::Hit(_)
        | BufferDecision::Loaded(_)
        | BufferDecision::Dirtied(_)
        | BufferDecision::Unpinned(_)
        | BufferDecision::Flushed { .. } => Ok(()),
        BufferDecision::Evicted { .. } => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("buffer unexpectedly evicted during relational page {action}"),
        )),
        BufferDecision::Rejected { reason, .. } => Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!("buffer {action} rejected for relational page: {reason}"),
        )),
    }
}

fn page_replay_error(decision: PageDecision) -> RsfError {
    match decision {
        PageDecision::Rejected { page, reason } => RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "relational page WAL replay rejected for {}.{}: {reason}",
                page.table, page.number
            ),
        ),
        other => RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("unexpected relational page WAL replay decision {other:?}"),
        ),
    }
}

fn checked_len_i32(bytes: &[u8]) -> RsfResult<i32> {
    checked_usize_i32(bytes.len())
}

fn checked_usize_i32(value: usize) -> RsfResult<i32> {
    i32::try_from(value).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "row payload length does not fit relational page delta",
        )
    })
}

fn commit_sql_request(
    router: &mut impl FabricRouterPort,
    pg: PlacementGroupId,
    request: ClientRequestId,
    operation: FabricOperation,
) -> RsfResult<CommitEvidence> {
    let commit = router.commit_request(pg, request, operation)?;
    Ok(commit_evidence(commit.certificate))
}

fn commit_evidence(certificate: CommitCertificate) -> CommitEvidence {
    CommitEvidence {
        placement_group: certificate.pg,
        log_index: certificate.index,
        version: certificate.version,
        consistency: match certificate.mode {
            ConsistencyMode::Strict => CommitConsistency::Strict,
            ConsistencyMode::AvailabilityFirst => CommitConsistency::AvailabilityFirst,
        },
        operation_id: certificate.operation_id,
    }
}

fn outcome_row_count(outcome: &SqlOutcome) -> usize {
    match outcome {
        SqlOutcome::Selected { rows } => rows.len(),
        SqlOutcome::Inserted { .. } | SqlOutcome::Updated { .. } | SqlOutcome::Deleted { .. } => 1,
        SqlOutcome::TableCreated { .. }
        | SqlOutcome::IndexCreated { .. }
        | SqlOutcome::TransactionBegan { .. }
        | SqlOutcome::SavepointCreated { .. }
        | SqlOutcome::SavepointRolledBack { .. }
        | SqlOutcome::TransactionCommitted { .. }
        | SqlOutcome::TransactionRolledBack { .. } => 0,
    }
}

fn transaction_id_from_sequence(sequence: u64) -> TransactionId {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&sequence.to_le_bytes());
    TransactionId(id)
}

fn transaction_id_number(id: &TransactionId) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&id.0[..8]);
    u64::from_le_bytes(bytes)
}

fn validate_savepoint_name(name: &str) -> RsfResult<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "savepoint name must contain only ASCII alphanumeric, '_' or '-' characters",
        ));
    }
    Ok(())
}

fn stable_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::{Epoch, NodeId};
    use rsf_executor::ExecutionSource;
    use rsf_planner::AccessPath;
    use rsf_protocol::{FabricRouter, VsrBackedFabricRouter};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn insert_materializes_row_and_kv_mutation() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();
        sql.create_table_with_columns("users", "id", ["id", "name"])
            .expect("create table");

        let row = sql
            .insert(
                &mut router,
                PlacementGroupId(1),
                "users",
                "1",
                BTreeMap::from([("name".to_string(), "ada".to_string())]),
            )
            .expect("insert");

        assert_eq!(row.version, CommitVersion(1));
        assert_eq!(
            sql.select_pk("users", "1").expect("row").columns["name"],
            "ada"
        );
        assert_eq!(router.committed().len(), 1);
    }

    #[test]
    fn insert_row_and_physical_kv_share_commit_version() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();
        sql.create_table_with_columns("users", "id", ["id", "name"])
            .expect("create table");

        let row = sql
            .insert(
                &mut router,
                PlacementGroupId(1),
                "users",
                "1",
                BTreeMap::from([("name".to_string(), "ada".to_string())]),
            )
            .expect("insert");
        let snapshot = sql.snapshot(&router);

        assert_eq!(row.version, CommitVersion(1));
        let encoded = sql
            .encoded_row_at("users", "1", snapshot)
            .expect("encoded row");
        assert_eq!(&encoded[..5], b"RSFR1");
        let decoded = RowCodec
            .decode(&sql.tables["users"].descriptor, encoded)
            .expect("decode row");
        assert_eq!(decoded.schema_version, CommitVersion(1));
        assert_eq!(decoded.values["id"], RowValue::Text("1".to_string()));
        assert_eq!(decoded.values["name"], RowValue::Text("ada".to_string()));
        assert_eq!(router.latest_version(), row.version);
    }

    #[test]
    fn sql_select_reads_ordered_kv_physical_rows_not_row_cache() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();
        sql.create_table_with_columns("users", "id", ["id", "name"])
            .expect("create table");
        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "1",
            BTreeMap::from([("name".to_string(), "ada".to_string())]),
        )
        .expect("insert ada");
        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "2",
            BTreeMap::from([("name".to_string(), "grace".to_string())]),
        )
        .expect("insert grace");

        sql.rows.clear();

        let SqlOutcome::Selected { rows } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE id = 1",
            )
            .expect("select primary")
        else {
            panic!("expected selected rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns["name"], "ada");

        let SqlOutcome::Selected { rows } = sql
            .execute(&mut router, PlacementGroupId(1), "SELECT * FROM users")
            .expect("select range")
        else {
            panic!("expected selected rows");
        };
        assert_eq!(
            rows.into_iter()
                .map(|row| row.columns["name"].clone())
                .collect::<Vec<_>>(),
            vec!["ada".to_string(), "grace".to_string()]
        );
        assert_eq!(sql.row_count(), 2);
    }

    #[test]
    fn sql_index_rebuild_and_stats_scan_ordered_kv_physical_rows() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();
        sql.create_table_with_columns("users", "id", ["id", "name"])
            .expect("create table");
        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "1",
            BTreeMap::from([("name".to_string(), "ada".to_string())]),
        )
        .expect("insert ada");
        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "2",
            BTreeMap::from([("name".to_string(), "grace".to_string())]),
        )
        .expect("insert grace");

        sql.rows.clear();
        sql.create_index("users", "name").expect("create index");

        let selected = sql.select_eq("users", "name", "grace");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].primary_key, "2");

        let estimate = sql
            .analyze_table("users", router.latest_version())
            .expect("analyze physical rows");
        assert_eq!(estimate.rows, 2);
        assert_eq!(estimate.version, CommitVersion(2));
        assert_eq!(sql.secondary_index_entry_count(), 2);
    }

    #[test]
    fn sql_mutations_drive_page_wal_and_buffer_layout() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();
        sql.create_table_with_columns("users", "id", ["id", "name"])
            .expect("create table");

        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "1",
            BTreeMap::from([("name".to_string(), "ada".to_string())]),
        )
        .expect("insert");
        sql.update(
            &mut router,
            PlacementGroupId(1),
            "users",
            "1",
            BTreeMap::from([("name".to_string(), "grace".to_string())]),
        )
        .expect("update");
        sql.delete(&mut router, PlacementGroupId(1), "users", "1")
            .expect("delete");

        let report = sql.page_wal_report();
        assert_eq!(report.pages, 1);
        assert_eq!(report.page_redo, 3);
        assert_eq!(report.wal_entries, 3);
        assert_eq!(report.durable_lsn, report.checkpoint_lsn);
        assert!(report.checkpoint_bytes >= 4096);
        assert_eq!(report.buffer_resident, 1);
        assert_eq!(report.ordered_kv_tables, 0);
        assert_eq!(report.page_wal_tables, 1);
        assert_eq!(report.checkpoint_live_rows, 0);
        assert_eq!(report.checkpoint_tombstones, 1);
    }

    #[test]
    fn oversized_sql_rows_are_rejected_before_protocol_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();
        sql.create_table_with_profile("users", "id", ["id", "name"], SqlLayoutProfile::PageWal)
            .unwrap();
        let before = router.latest_version();
        assert!(
            sql.insert(
                &mut router,
                PlacementGroupId(1),
                "users",
                "1",
                BTreeMap::from([("name".into(), "x".repeat(5000))])
            )
            .is_err()
        );
        assert_eq!(router.latest_version(), before);
        assert_eq!(sql.page_wal_report().pages, 0);
        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "1",
            BTreeMap::from([("name".into(), "old".into())]),
        )
        .unwrap();
        let before = router.latest_version();
        assert!(
            sql.update(
                &mut router,
                PlacementGroupId(1),
                "users",
                "1",
                BTreeMap::from([("name".into(), "x".repeat(5000))])
            )
            .is_err()
        );
        assert_eq!(router.latest_version(), before);
        assert_eq!(sql.select_pk("users", "1").unwrap().columns["name"], "old");
    }

    #[test]
    fn invalid_multipage_snapshot_does_not_replace_live_sql_state() {
        let mut sql = SqlEngine::default();
        sql.create_table_with_profile("users", "id", ["id", "name"], SqlLayoutProfile::PageWal)
            .unwrap();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "1",
            BTreeMap::from([("name".into(), "old".into())]),
        )
        .unwrap();
        let before = sql.page_wal_snapshot();
        let mut image =
            rsf_page::PageImage::new(PageId::new("users", 2), CommitVersion(2), 4096).unwrap();
        image
            .insert_row(logical_row_key("users", "1"), b"duplicate".to_vec())
            .unwrap();
        let mut bad = sql.page_wal_snapshot();
        bad.page_images.push(image.encode().unwrap());
        assert!(sql.recover_page_wal_snapshot(bad).is_err());
        assert_eq!(sql.page_wal_snapshot(), before);
        assert_eq!(sql.select_pk("users", "1").unwrap().columns["name"], "old");
    }

    #[test]
    fn sql_page_wal_grows_moves_rows_and_recovers_many_pages() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();
        sql.create_table_with_profile("users", "id", ["id", "name"], SqlLayoutProfile::PageWal)
            .unwrap();
        for n in 0..160 {
            sql.insert(
                &mut router,
                PlacementGroupId(1),
                "users",
                n.to_string(),
                BTreeMap::from([("name".to_string(), "x".repeat(2048))]),
            )
            .unwrap();
        }
        assert_eq!(sql.page_wal_report().pages, 160);
        assert!(sql.page_wal_report().buffer_resident <= 128);
        // Fill the last page, then grow the small row until relocation is required.
        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "small",
            BTreeMap::from([("name".to_string(), "a".repeat(100))]),
        )
        .unwrap();
        let small_key = logical_row_key("users", "small");
        let old_page = sql.row_pages[&small_key].clone();
        sql.update(
            &mut router,
            PlacementGroupId(1),
            "users",
            "small",
            BTreeMap::from([("name".to_string(), "b".repeat(3000))]),
        )
        .unwrap();
        assert_ne!(sql.row_pages[&small_key], old_page);
        sql.delete(&mut router, PlacementGroupId(1), "users", "42")
            .unwrap();
        let snapshot = sql.page_wal_snapshot();
        let mut restarted = SqlEngine::default();
        restarted
            .create_table_with_profile("users", "id", ["id", "name"], SqlLayoutProfile::PageWal)
            .unwrap();
        restarted.recover_page_wal_snapshot(snapshot).unwrap();
        assert_eq!(restarted.row_pages, sql.row_pages);
        let (mut canonical, recovery) = SqlEngine::recover_from_committed_kv(
            sql.committed_kv_for_recovery(),
            sql.recovered_tables_for_checkpoint(),
        )
        .unwrap();
        assert_eq!(recovery.live_rows, 160);
        canonical
            .update(
                &mut router,
                PlacementGroupId(1),
                "users",
                "small",
                BTreeMap::from([("name".into(), "after-restart".into())]),
            )
            .unwrap();
        assert_eq!(
            canonical.select_pk("users", "small").unwrap().columns["name"],
            "after-restart"
        );
        assert_eq!(restarted.page_wal_report().checkpoint_live_rows, 160);
        assert_eq!(
            restarted
                .pages
                .read_image(&restarted.row_pages[&small_key], CommitVersion(0))
                .unwrap(),
            sql.pages
                .read_image(&sql.row_pages[&small_key], CommitVersion(0))
                .unwrap()
        );
        restarted
            .apply_page_wal(
                "users",
                RedoOperation::UpdateRow,
                CommitVersion(1000),
                16,
                small_key.clone(),
                Some(b"physical-update".to_vec()),
            )
            .unwrap();
    }

    #[test]
    fn sql_page_wal_snapshot_replays_after_crash_restart() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();
        sql.create_table_with_profile("users", "id", ["id", "name"], SqlLayoutProfile::PageWal)
            .expect("create table");

        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "1",
            BTreeMap::from([("name".to_string(), "ada".to_string())]),
        )
        .expect("insert");
        sql.update(
            &mut router,
            PlacementGroupId(1),
            "users",
            "1",
            BTreeMap::from([("name".to_string(), "grace".to_string())]),
        )
        .expect("update");
        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "2",
            BTreeMap::from([("name".to_string(), "alan".to_string())]),
        )
        .expect("insert second row");

        let snapshot = sql.page_wal_snapshot();
        assert_eq!(snapshot.page_images.len(), 1);
        assert_eq!(snapshot.redo_records.len(), 3);

        let mut restarted = SqlEngine::default();
        restarted
            .create_table_with_profile("users", "id", ["id", "name"], SqlLayoutProfile::PageWal)
            .expect("register page-wal table");
        let replay = restarted
            .recover_page_wal_snapshot(snapshot)
            .expect("recover page wal");

        assert_eq!(replay.pages, 1);
        assert_eq!(replay.decoded_images, 1);
        assert_eq!(replay.decoded_records, 3);
        assert_eq!(replay.applied_records, 3);
        assert_eq!(replay.skipped_records, 0);
        assert_eq!(replay.high_lsn, CommitVersion(3));

        let report = restarted.page_wal_report();
        assert_eq!(report.pages, 1);
        assert_eq!(report.page_redo, 3);
        assert_eq!(report.checkpoint_lsn, CommitVersion(3));
        assert_eq!(report.checkpoint_live_rows, 2);
        assert_eq!(report.checkpoint_tombstones, 0);
    }

    #[test]
    fn sql_layout_profile_selects_page_wal_and_ordered_kv_tables() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();
        sql.create_table_with_profile("users", "id", ["id", "name"], SqlLayoutProfile::PageWal)
            .expect("create page-wal table");
        sql.create_table_with_profile(
            "events",
            "id",
            ["id", "payload"],
            SqlLayoutProfile::OrderedKv,
        )
        .expect("create ordered-kv table");

        assert_eq!(
            sql.table_layout_decision("users")
                .expect("users layout")
                .layout,
            SqlTableLayout::PageWal
        );
        assert_eq!(
            sql.table_layout_decision("events")
                .expect("events layout")
                .layout,
            SqlTableLayout::OrderedKv
        );

        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "users",
            "1",
            BTreeMap::from([("name".to_string(), "ada".to_string())]),
        )
        .expect("insert page-wal row");
        sql.insert(
            &mut router,
            PlacementGroupId(1),
            "events",
            "evt-1",
            BTreeMap::from([("payload".to_string(), "opened".to_string())]),
        )
        .expect("insert ordered-kv row");

        assert_eq!(
            sql.select_pk("users", "1").expect("users row").columns["name"],
            "ada"
        );
        assert_eq!(
            sql.select_pk("events", "evt-1")
                .expect("events row")
                .columns["payload"],
            "opened"
        );

        let report = sql.page_wal_report();
        assert_eq!(report.pages, 1);
        assert_eq!(report.page_redo, 1);
        assert_eq!(report.wal_entries, 1);
        assert_eq!(report.ordered_kv_tables, 1);
        assert_eq!(report.page_wal_tables, 1);
    }

    #[test]
    fn sql_transaction_mutations_share_commit_version_but_advance_page_lsn() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(&mut router, PlacementGroupId(1), "BEGIN")
            .expect("begin");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("stage ada");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (2, 'grace')",
        )
        .expect("stage grace");
        sql.execute(&mut router, PlacementGroupId(1), "COMMIT")
            .expect("commit");

        assert_eq!(
            sql.select_pk("users", "1").expect("ada").version,
            CommitVersion(2)
        );
        assert_eq!(
            sql.select_pk("users", "2").expect("grace").version,
            CommitVersion(2)
        );
        let report = sql.page_wal_report();
        assert_eq!(report.page_redo, 2);
        assert_eq!(report.wal_entries, 2);
        assert_eq!(report.checkpoint_lsn, CommitVersion(3));
        assert_eq!(report.durable_lsn, CommitVersion(3));
    }

    #[test]
    fn executes_tiny_create_insert_select_sql_surface() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        let SqlOutcome::TableCreated {
            table,
            version,
            evidence,
        } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "CREATE TABLE users(id PRIMARY KEY, name)",
            )
            .expect("create")
        else {
            panic!("expected table creation");
        };
        assert_eq!(table, "users");
        assert_eq!(version, CommitVersion(1));
        assert_eq!(evidence.version, CommitVersion(1));
        assert_eq!(evidence.placement_group, PlacementGroupId(1));

        let insert = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users(id, name) VALUES (1, 'ada')",
            )
            .expect("insert");
        let SqlOutcome::Inserted { row, evidence } = insert else {
            panic!("expected insert outcome");
        };
        assert_eq!(row.version, CommitVersion(2));
        assert_eq!(evidence.version(), CommitVersion(2));

        let select = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE id = 1",
            )
            .expect("select");
        let SqlOutcome::Selected { rows } = select else {
            panic!("expected select outcome");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns["name"], "ada");
        assert_eq!(sql.query_report_count(), 1);
        let report = sql.last_query_report().expect("query report");
        assert!(matches!(
            report.physical.operators[0].access,
            AccessPath::PrimaryKeyLookup { .. }
        ));
        assert_eq!(
            report.execution.stages[0].source,
            ExecutionSource::PrimaryIndex
        );
    }

    #[test]
    fn prepared_portal_executes_bound_select_through_sql_engine() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        assert_eq!(sql.prepared_schema_version(), CommitVersion(1));
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert row");

        assert_eq!(
            sql.prepare_statement(
                "select_user",
                "SELECT name FROM users WHERE id = $1",
                vec![PreparedParamType::Int64],
            ),
            vec![PortalDecision::Parsed {
                statement: "select_user".to_string(),
                parameters: 1,
                schema_version: CommitVersion(1),
            }]
        );
        assert!(matches!(
            sql.bind_portal(
                "portal_select_user",
                "select_user",
                vec![PreparedValue::Int64(1)]
            ),
            PortalDecision::Bound { .. }
        ));
        assert_eq!(
            sql.describe_portal("portal_select_user"),
            PortalDecision::Described {
                portal: "portal_select_user".to_string(),
                columns: 1,
                parameters: 1,
            }
        );

        let execution = sql
            .execute_portal(&mut router, PlacementGroupId(1), "portal_select_user", 10)
            .expect("execute portal");
        assert_eq!(
            execution.decision,
            PortalDecision::Executed {
                portal: "portal_select_user".to_string(),
                rows: 1,
                elapsed_ms: 1,
            }
        );
        let SqlOutcome::Selected { rows } = execution.outcome else {
            panic!("expected selected rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns["name"], "ada");
        assert_eq!(
            sql.close_portal("portal_select_user"),
            PortalDecision::ClosedPortal {
                portal: "portal_select_user".to_string(),
            }
        );
        assert_eq!(
            sql.close_statement("select_user"),
            PortalDecision::ClosedStatement {
                statement: "select_user".to_string(),
            }
        );
    }

    #[test]
    fn prepared_statement_invalidates_on_schema_change_not_dml() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.prepare_statement(
            "select_user",
            "SELECT id FROM users WHERE id = $1",
            vec![PreparedParamType::Int64],
        );
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert row");
        assert!(matches!(
            sql.bind_portal(
                "portal_after_dml",
                "select_user",
                vec![PreparedValue::Int64(1)]
            ),
            PortalDecision::Bound { .. }
        ));

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE INDEX users_name ON users(name)",
        )
        .expect("create index");
        assert_eq!(sql.prepared_schema_version(), CommitVersion(3));
        assert_eq!(
            sql.bind_portal(
                "portal_after_schema_change",
                "select_user",
                vec![PreparedValue::Int64(1)],
            ),
            PortalDecision::Rejected {
                target: "select_user".to_string(),
                reason: "statement invalidated: prepared_version=1 current_version=3".to_string(),
            }
        );
    }

    #[test]
    fn sql_transaction_commits_staged_writes_atomically() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");

        assert!(matches!(
            sql.execute(&mut router, PlacementGroupId(1), "BEGIN")
                .expect("begin"),
            SqlOutcome::TransactionBegan {
                snapshot: CommitVersion(1),
                ..
            }
        ));
        let staged = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users(id, name) VALUES (1, 'ada')",
            )
            .expect("stage ada");
        let SqlOutcome::Inserted { row, evidence } = staged else {
            panic!("expected staged insert");
        };
        assert_eq!(row.version, CommitVersion(1));
        assert_eq!(
            evidence,
            SqlMutationEvidence::Staged {
                session_id: "sql-txn-1".to_string(),
                snapshot: CommitVersion(1),
            }
        );
        assert_eq!(router.latest_version(), CommitVersion(1));
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (2, 'grace')",
        )
        .expect("stage grace");
        assert_eq!(router.latest_version(), CommitVersion(1));

        let SqlOutcome::Selected { rows } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE id = 2",
            )
            .expect("transactional select")
        else {
            panic!("expected selected rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns["name"], "grace");

        let SqlOutcome::TransactionCommitted {
            evidence,
            mutations,
            ..
        } = sql
            .execute(&mut router, PlacementGroupId(1), "COMMIT")
            .expect("commit")
        else {
            panic!("expected transaction commit");
        };
        assert_eq!(mutations, 2);
        assert_eq!(evidence.version, CommitVersion(2));
        assert_eq!(router.latest_version(), CommitVersion(2));
        assert_eq!(
            sql.select_pk("users", "1").expect("ada").version,
            CommitVersion(2)
        );
        assert_eq!(
            sql.select_pk("users", "2").expect("grace").version,
            CommitVersion(2)
        );
    }

    #[test]
    fn sql_transaction_rollback_discards_overlay_without_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(&mut router, PlacementGroupId(1), "BEGIN")
            .expect("begin");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("stage ada");
        let SqlOutcome::Selected { rows } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE id = 1",
            )
            .expect("overlay read")
        else {
            panic!("expected selected rows");
        };
        assert_eq!(rows.len(), 1);

        let SqlOutcome::TransactionRolledBack { discarded, .. } = sql
            .execute(&mut router, PlacementGroupId(1), "ROLLBACK")
            .expect("rollback")
        else {
            panic!("expected rollback");
        };
        assert_eq!(discarded, 1);
        assert_eq!(router.latest_version(), CommitVersion(1));
        assert!(sql.select_pk("users", "1").is_none());
    }

    #[test]
    fn sql_transaction_savepoint_rewinds_staged_changes() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(&mut router, PlacementGroupId(1), "BEGIN")
            .expect("begin");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("stage ada");
        assert!(matches!(
            sql.execute(&mut router, PlacementGroupId(1), "SAVEPOINT after_ada")
                .expect("savepoint"),
            SqlOutcome::SavepointCreated { staged: 1, .. }
        ));
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET name = 'grace' WHERE id = 1",
        )
        .expect("stage update");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (2, 'lin')",
        )
        .expect("stage lin");
        assert!(matches!(
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "ROLLBACK TO SAVEPOINT after_ada",
            )
            .expect("rollback to savepoint"),
            SqlOutcome::SavepointRolledBack { staged: 1, .. }
        ));
        let SqlOutcome::Selected { rows } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE id = 1",
            )
            .expect("select ada")
        else {
            panic!("expected selected rows");
        };
        assert_eq!(rows[0].columns["name"], "ada");
        let SqlOutcome::Selected { rows } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE id = 2",
            )
            .expect("select lin")
        else {
            panic!("expected selected rows");
        };
        assert!(rows.is_empty());

        let SqlOutcome::TransactionCommitted { mutations, .. } = sql
            .execute(&mut router, PlacementGroupId(1), "COMMIT")
            .expect("commit")
        else {
            panic!("expected commit");
        };
        assert_eq!(mutations, 1);
        assert_eq!(
            sql.select_pk("users", "1").expect("ada").columns["name"],
            "ada"
        );
        assert!(sql.select_pk("users", "2").is_none());
    }

    #[test]
    fn sql_analyze_publishes_stats_used_by_physical_planner() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name, active)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name, active) VALUES (1, 'ada', true)",
        )
        .expect("insert ada");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name, active) VALUES (2, 'grace', true)",
        )
        .expect("insert grace");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name, active) VALUES (3, 'ada', false)",
        )
        .expect("insert second ada");

        let estimate = sql
            .analyze_table("users", router.latest_version())
            .expect("analyze table");
        assert_eq!(estimate.rows, 3);
        assert_eq!(estimate.version, CommitVersion(4));
        let name = estimate
            .columns
            .iter()
            .find(|column| column.column == "name")
            .expect("name stats");
        assert_eq!(name.distinct_values, 2);
        assert!(matches!(
            sql.stats_freshness("users", CommitVersion(5), 1).unwrap(),
            Freshness::Fresh
        ));
        assert!(matches!(
            sql.stats_freshness("users", CommitVersion(7), 1).unwrap(),
            Freshness::Stale { lag: 3 }
        ));

        let SqlOutcome::Selected { rows } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE active = true",
            )
            .expect("select")
        else {
            panic!("expected selected rows");
        };
        assert_eq!(rows.len(), 2);
        let report = sql.last_query_report().expect("query report");
        assert_eq!(report.physical.estimated_rows, estimate.rows);
        assert_eq!(
            report.physical.operators[0].estimated_bytes,
            estimate.rows * estimate.average_row_bytes().max(1)
        );
    }

    #[test]
    fn sql_vacuum_prunes_obsolete_tuple_versions() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET name = 'grace' WHERE id = 1",
        )
        .expect("update");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET name = 'lin' WHERE id = 1",
        )
        .expect("second update");
        assert_eq!(sql.tuple_version_count(), 3);
        assert_eq!(
            sql.select_pk_at("users", "1", CommitVersion(2))
                .expect("historical row")
                .columns["name"],
            "ada"
        );

        let report = sql
            .vacuum_table("users", CommitVersion(4), 1)
            .expect("vacuum table");
        assert_eq!(report.chains, 1);
        assert_eq!(report.pruned_versions, 2);
        assert_eq!(report.retained_versions, 1);
        assert!(report.reclaimed_bytes > 0);
        assert!(report.rejections.is_empty());
        assert_eq!(sql.tuple_version_count(), 1);
        assert!(sql.select_pk_at("users", "1", CommitVersion(2)).is_none());
        assert_eq!(
            sql.select_pk_at("users", "1", CommitVersion(4))
                .expect("current row")
                .columns["name"],
            "lin"
        );
    }

    #[test]
    fn sql_select_uses_query_planner_projection_limit_and_range_execution() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name, active)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name, active) VALUES (1, 'ada', true)",
        )
        .expect("insert ada");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name, active) VALUES (2, 'grace', true)",
        )
        .expect("insert grace");

        let SqlOutcome::Selected { rows } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT name FROM users LIMIT 1",
            )
            .expect("select")
        else {
            panic!("expected select");
        };

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].columns.keys().cloned().collect::<Vec<_>>(),
            vec!["name".to_string()]
        );
        let report = sql.last_query_report().expect("query report");
        assert!(matches!(
            report.physical.operators[0].access,
            AccessPath::RangeScan
        ));
        assert_eq!(report.execution.stages[0].source, ExecutionSource::Range);
        assert_eq!(report.execution.rows, 1);
    }

    #[test]
    fn sql_surface_uses_declared_primary_key_not_hardcoded_id() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE accounts(email PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO accounts(email, name) VALUES ('ada@example.com', 'ada')",
        )
        .expect("insert");

        let SqlOutcome::Updated { row, evidence } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "UPDATE accounts SET name = 'lovelace' WHERE email = 'ada@example.com'",
            )
            .expect("update")
        else {
            panic!("expected update");
        };
        assert_eq!(row.primary_key, "ada@example.com");
        assert_eq!(row.columns["name"], "lovelace");
        assert_eq!(evidence.version(), CommitVersion(3));

        let SqlOutcome::Selected { rows } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM accounts WHERE email = 'ada@example.com'",
            )
            .expect("select")
        else {
            panic!("expected select");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].primary_key, "ada@example.com");

        let SqlOutcome::Deleted { primary_key, .. } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "DELETE FROM accounts WHERE email = 'ada@example.com'",
            )
            .expect("delete")
        else {
            panic!("expected delete");
        };
        assert_eq!(primary_key, "ada@example.com");
        assert_eq!(sql.row_count(), 0);
    }

    #[test]
    fn sql_mutations_reject_non_primary_key_predicates() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE accounts(email PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO accounts(email, name) VALUES ('ada@example.com', 'ada')",
        )
        .expect("insert");

        let err = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "UPDATE accounts SET name = 'lovelace' WHERE name = 'ada'",
            )
            .expect_err("non-primary-key mutation predicate must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("must target primary key column email"));
    }

    #[test]
    fn sql_surface_rejects_columns_outside_table_descriptor() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");

        let insert_err = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users(id, nickname) VALUES (1, 'ada')",
            )
            .expect_err("unknown insert column must fail");
        assert_eq!(insert_err.code, ErrorCode::InvalidInput);
        assert!(
            insert_err
                .message
                .contains("column nickname is not declared")
        );

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert valid row");

        let update_err = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "UPDATE users SET nickname = 'a' WHERE id = 1",
            )
            .expect_err("unknown update column must fail");
        assert_eq!(update_err.code, ErrorCode::InvalidInput);
        assert!(
            update_err
                .message
                .contains("column nickname is not declared")
        );

        let index_err = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "CREATE INDEX users_nick ON users(nickname)",
            )
            .expect_err("unknown index column must fail");
        assert_eq!(index_err.code, ErrorCode::InvalidInput);
        assert!(
            index_err
                .message
                .contains("column nickname is not declared")
        );
    }

    #[test]
    fn sql_surface_uses_durable_catalog_table_descriptor() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut catalog = CatalogEngine::default();
        let mut sql = SqlEngine::default();

        catalog
            .create_namespace(&mut router, PlacementGroupId(1), "app")
            .expect("namespace");
        catalog
            .create_table(
                &mut router,
                PlacementGroupId(1),
                TableDescriptor {
                    namespace: NamespaceName("app".to_string()),
                    name: TableName("accounts".to_string()),
                    columns: vec![
                        ColumnDescriptor {
                            name: "email".to_string(),
                            ty: ColumnType::Text,
                            nullable: false,
                        },
                        ColumnDescriptor {
                            name: "name".to_string(),
                            ty: ColumnType::Text,
                            nullable: false,
                        },
                    ],
                    primary_key: vec!["email".to_string()],
                },
            )
            .expect("catalog table");

        sql.register_catalog_table(&catalog, "app", "accounts")
            .expect("register catalog table");
        let SqlOutcome::Inserted { row, evidence } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO accounts(email, name) VALUES ('ada@example.com', 'ada')",
            )
            .expect("insert through catalog descriptor")
        else {
            panic!("expected insert");
        };

        assert_eq!(row.primary_key, "ada@example.com");
        assert_eq!(row.columns["name"], "ada");
        assert_eq!(evidence.version(), CommitVersion(3));
        assert_eq!(router.latest_version(), CommitVersion(3));

        let err = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO accounts(email, nickname) VALUES ('grace@example.com', 'g')",
            )
            .expect_err("catalog descriptor must reject unknown columns");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("column nickname is not declared"));
        assert_eq!(router.latest_version(), CommitVersion(3));
    }

    #[test]
    fn sql_physical_rows_use_descriptor_typed_row_codec() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.register_table_descriptor(TableDescriptor {
            namespace: NamespaceName("app".to_string()),
            name: TableName("accounts".to_string()),
            columns: vec![
                ColumnDescriptor {
                    name: "account_id".to_string(),
                    ty: ColumnType::Int64,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "name".to_string(),
                    ty: ColumnType::Text,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "active".to_string(),
                    ty: ColumnType::Bool,
                    nullable: false,
                },
            ],
            primary_key: vec!["account_id".to_string()],
        })
        .expect("register descriptor");

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO accounts(account_id, name, active) VALUES (7, 'ada', true)",
        )
        .expect("insert typed row");
        let snapshot = sql.snapshot(&router);
        let encoded = sql
            .encoded_row_at("accounts", "7", snapshot)
            .expect("encoded row");
        assert_eq!(&encoded[..5], b"RSFR1");

        let decoded = RowCodec
            .decode(&sql.tables["accounts"].descriptor, encoded)
            .expect("decode typed row");
        assert_eq!(decoded.schema_version, CommitVersion(1));
        assert_eq!(decoded.values["account_id"], RowValue::Int64(7));
        assert_eq!(decoded.values["name"], RowValue::Text("ada".to_string()));
        assert_eq!(decoded.values["active"], RowValue::Bool(true));

        let bad_type = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO accounts(account_id, name, active) VALUES ('oops', 'grace', true)",
            )
            .expect_err("bad typed primary key must fail before commit");
        assert_eq!(bad_type.code, ErrorCode::InvalidInput);
        assert!(bad_type.message.contains("account_id expected int64"));

        let missing_required = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO accounts(account_id, name) VALUES (8, 'grace')",
            )
            .expect_err("missing non-null column must fail before commit");
        assert_eq!(missing_required.code, ErrorCode::InvalidInput);
        assert!(missing_required.message.contains("not-null:active"));
        assert_eq!(router.latest_version(), CommitVersion(1));
    }

    #[test]
    fn sql_constraints_reject_null_and_unique_violations_before_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.register_table_descriptor(TableDescriptor {
            namespace: NamespaceName("app".to_string()),
            name: TableName("users".to_string()),
            columns: vec![
                ColumnDescriptor {
                    name: "id".to_string(),
                    ty: ColumnType::Text,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "email".to_string(),
                    ty: ColumnType::Text,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "name".to_string(),
                    ty: ColumnType::Text,
                    nullable: true,
                },
            ],
            primary_key: vec!["id".to_string()],
        })
        .expect("register table");
        sql.add_unique_constraint("users", "users_email", ["email"])
            .expect("unique constraint");

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, email, name) VALUES ('1', 'ada@example.com', 'ada')",
        )
        .expect("insert first row");
        assert_eq!(sql.unique_constraint_key_count(), 1);
        assert_eq!(sql.referenced_constraint_key_count(), 1);

        let duplicate = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users(id, email, name) VALUES ('2', 'ada@example.com', 'copy')",
            )
            .expect_err("duplicate email must fail");
        assert_eq!(duplicate.code, ErrorCode::InvalidInput);
        assert!(duplicate.message.contains("unique:users_email"));

        let missing_email = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users(id, name) VALUES ('3', 'missing email')",
            )
            .expect_err("missing required email must fail");
        assert_eq!(missing_email.code, ErrorCode::InvalidInput);
        assert!(missing_email.message.contains("not-null:email"));
        assert_eq!(router.latest_version(), CommitVersion(1));
    }

    #[test]
    fn sql_constraints_update_and_delete_constraint_state() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, email, name)",
        )
        .expect("create table");
        sql.add_unique_constraint("users", "users_email", ["email"])
            .expect("unique constraint");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, email, name) VALUES (1, 'ada@example.com', 'ada')",
        )
        .expect("insert");

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET email = 'lovelace@example.com' WHERE id = 1",
        )
        .expect("update unique key");
        assert_eq!(sql.unique_constraint_key_count(), 1);

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, email, name) VALUES (2, 'ada@example.com', 'new ada')",
        )
        .expect("old unique key was released");
        assert_eq!(sql.unique_constraint_key_count(), 2);

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "DELETE FROM users WHERE id = 1",
        )
        .expect("delete releases unique key");
        assert_eq!(sql.unique_constraint_key_count(), 1);
        assert_eq!(sql.referenced_constraint_key_count(), 1);
    }

    #[test]
    fn sql_constraints_enforce_foreign_keys_against_referenced_rows() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE orgs(id PRIMARY KEY, name)",
        )
        .expect("create orgs");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, org_id, name)",
        )
        .expect("create users");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO orgs(id, name) VALUES ('research', 'Research')",
        )
        .expect("insert referenced org");
        sql.add_foreign_key_constraint("users", "users_org", ["org_id"], "orgs", ["id"])
            .expect("foreign key");

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, org_id, name) VALUES (1, 'research', 'ada')",
        )
        .expect("insert with valid fk");

        let missing = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users(id, org_id, name) VALUES (2, 'missing', 'grace')",
            )
            .expect_err("missing foreign key must fail");
        assert_eq!(missing.code, ErrorCode::InvalidInput);
        assert!(missing.message.contains("foreign-key:users_org"));
        assert_eq!(router.latest_version(), CommitVersion(4));
    }

    #[test]
    fn sql_tuple_visibility_reads_historical_row_versions() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET name = 'grace' WHERE id = 1",
        )
        .expect("update");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "DELETE FROM users WHERE id = 1",
        )
        .expect("delete");

        assert!(sql.select_pk_at("users", "1", CommitVersion(1)).is_none());
        let inserted = sql
            .select_pk_at("users", "1", CommitVersion(2))
            .expect("inserted version visible");
        assert_eq!(inserted.columns["name"], "ada");
        assert_eq!(inserted.version, CommitVersion(2));

        let updated = sql
            .select_pk_at("users", "1", CommitVersion(3))
            .expect("updated version visible");
        assert_eq!(updated.columns["name"], "grace");
        assert_eq!(updated.version, CommitVersion(3));

        assert!(sql.select_pk_at("users", "1", CommitVersion(4)).is_none());
        assert_eq!(sql.tuple_version_count(), 2);
        assert_eq!(router.latest_version(), CommitVersion(4));
    }

    #[test]
    fn duplicate_sql_insert_is_rejected_before_tuple_or_protocol_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert");

        let err = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users(id, name) VALUES (1, 'grace')",
            )
            .expect_err("duplicate insert must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("already present"));
        assert_eq!(router.latest_version(), CommitVersion(2));
        assert_eq!(sql.tuple_version_count(), 1);
        assert_eq!(sql.primary_index_len(), 1);
    }

    #[test]
    fn sql_primary_key_reads_are_served_through_primary_index() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert");

        assert_eq!(sql.primary_index_len(), 1);
        assert_eq!(
            sql.select_pk("users", "1").expect("indexed row").columns["name"],
            "ada"
        );
        assert_eq!(sql.select_eq("users", "id", "1").len(), 1);

        sql.primary = PrimaryIndex::default();

        assert!(
            sql.rows
                .contains_key(&("users".to_string(), "1".to_string())),
            "row body remains present while primary index is missing"
        );
        assert!(sql.select_pk("users", "1").is_none());
        assert!(sql.select_eq("users", "id", "1").is_empty());
    }

    #[test]
    fn sql_primary_index_tracks_update_and_delete_lifecycle() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert");
        assert_eq!(sql.primary_index_len(), 1);

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET name = 'grace' WHERE id = 1",
        )
        .expect("update");
        assert_eq!(sql.primary_index_len(), 1);
        assert_eq!(
            sql.select_pk("users", "1").expect("updated row").columns["name"],
            "grace"
        );

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "DELETE FROM users WHERE id = 1",
        )
        .expect("delete");
        assert_eq!(sql.primary_index_len(), 0);
        assert!(sql.select_pk("users", "1").is_none());
        assert_eq!(router.latest_version(), CommitVersion(4));
    }

    #[test]
    fn sql_secondary_index_reads_are_served_through_index_store() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE INDEX users_name ON users(name)",
        )
        .expect("create index");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert");

        assert_eq!(sql.secondary_index_entry_count(), 1);
        assert_eq!(sql.select_eq("users", "name", "ada").len(), 1);

        sql.indexes = IndexStore::default();

        assert!(
            sql.rows
                .contains_key(&("users".to_string(), "1".to_string())),
            "row body remains present while secondary index is missing"
        );
        assert!(sql.select_eq("users", "name", "ada").is_empty());
        assert_eq!(
            sql.select_pk("users", "1").expect("primary row").columns["name"],
            "ada"
        );
    }

    #[test]
    fn sql_secondary_index_rebuilds_existing_rows_on_create_index() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert ada");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (2, 'ada')",
        )
        .expect("insert ada again");

        assert!(sql.select_eq("users", "name", "ada").is_empty());
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE INDEX users_name ON users(name)",
        )
        .expect("create index");

        assert_eq!(sql.secondary_index_entry_count(), 2);
        let rows = sql.select_eq("users", "name", "ada");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].primary_key, "1");
        assert_eq!(rows[1].primary_key, "2");
    }

    #[test]
    fn sql_secondary_index_skips_missing_nullable_values_until_update_sets_them() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name, nickname)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE INDEX users_nickname ON users(nickname)",
        )
        .expect("create index");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert without nullable indexed value");

        assert_eq!(sql.secondary_index_entry_count(), 0);
        assert!(sql.select_eq("users", "nickname", "countess").is_empty());

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET nickname = 'countess' WHERE id = 1",
        )
        .expect("set indexed value");

        assert_eq!(sql.secondary_index_entry_count(), 1);
        assert_eq!(sql.select_eq("users", "nickname", "countess").len(), 1);
    }

    #[test]
    fn sql_row_and_indexes_remain_atomic_when_protocol_commit_rejects_mutation() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE INDEX users_name ON users(name)",
        )
        .expect("create index");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert");

        let mut rejecting_router = FabricRouter::with_groups([PlacementGroupId(99)]);
        let update_err = sql
            .execute(
                &mut rejecting_router,
                PlacementGroupId(1),
                "UPDATE users SET name = 'grace' WHERE id = 1",
            )
            .expect_err("unmapped pg rejects update before state swap");
        assert_eq!(update_err.code, ErrorCode::InvalidInput);
        assert_eq!(
            sql.select_pk("users", "1").expect("original row").columns["name"],
            "ada"
        );
        assert_eq!(sql.select_eq("users", "name", "ada").len(), 1);
        assert!(sql.select_eq("users", "name", "grace").is_empty());
        assert_eq!(sql.primary_index_len(), 1);
        assert_eq!(sql.secondary_index_entry_count(), 1);

        let delete_err = sql
            .execute(
                &mut rejecting_router,
                PlacementGroupId(1),
                "DELETE FROM users WHERE id = 1",
            )
            .expect_err("unmapped pg rejects delete before state swap");
        assert_eq!(delete_err.code, ErrorCode::InvalidInput);
        assert_eq!(
            sql.select_pk("users", "1")
                .expect("row survives delete reject")
                .columns["name"],
            "ada"
        );
        assert_eq!(sql.select_eq("users", "name", "ada").len(), 1);
        assert_eq!(sql.primary_index_len(), 1);
        assert_eq!(sql.secondary_index_entry_count(), 1);

        let insert_err = sql
            .execute(
                &mut rejecting_router,
                PlacementGroupId(1),
                "INSERT INTO users(id, name) VALUES (2, 'grace')",
            )
            .expect_err("unmapped pg rejects insert before state swap");
        assert_eq!(insert_err.code, ErrorCode::InvalidInput);
        assert!(sql.select_pk("users", "2").is_none());
        assert!(sql.select_eq("users", "name", "grace").is_empty());
        assert_eq!(sql.row_count(), 1);
        assert_eq!(router.latest_version(), CommitVersion(3));
    }

    #[test]
    fn sql_transaction_commit_rejection_preserves_staged_overlay_without_partial_indexes() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE INDEX users_name ON users(name)",
        )
        .expect("create index");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert");
        sql.execute(&mut router, PlacementGroupId(1), "BEGIN")
            .expect("begin");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET name = 'grace' WHERE id = 1",
        )
        .expect("stage update");

        let mut rejecting_router = FabricRouter::with_groups([PlacementGroupId(99)]);
        let err = sql
            .execute(&mut rejecting_router, PlacementGroupId(1), "COMMIT")
            .expect_err("unmapped pg rejects transaction commit");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(sql.transaction_active());
        assert_eq!(
            sql.select_pk("users", "1")
                .expect("committed row unchanged")
                .columns["name"],
            "ada"
        );
        assert_eq!(sql.select_eq("users", "name", "ada").len(), 1);
        assert!(sql.select_eq("users", "name", "grace").is_empty());
        assert_eq!(sql.primary_index_len(), 1);
        assert_eq!(sql.secondary_index_entry_count(), 1);

        let committed = sql
            .execute(&mut router, PlacementGroupId(1), "COMMIT")
            .expect("retry commit on mapped pg");
        let SqlOutcome::TransactionCommitted { mutations, .. } = committed else {
            panic!("expected transaction commit");
        };
        assert_eq!(mutations, 1);
        assert!(!sql.transaction_active());
        assert_eq!(
            sql.select_pk("users", "1").expect("updated row").columns["name"],
            "grace"
        );
        assert!(sql.select_eq("users", "name", "ada").is_empty());
        assert_eq!(sql.select_eq("users", "name", "grace").len(), 1);
    }

    #[test]
    fn sql_recovery_rebuilds_rows_indexes_constraints_and_tuple_history_from_kv_versions() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.add_unique_constraint("users", "users_name_unique", ["name"])
            .expect("unique");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE INDEX users_name ON users(name)",
        )
        .expect("create index");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert ada");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (2, 'grace')",
        )
        .expect("insert grace");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET name = 'alan' WHERE id = 1",
        )
        .expect("update ada");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "DELETE FROM users WHERE id = 2",
        )
        .expect("delete grace");

        let kv = sql.committed_kv_for_recovery();
        let tables = recovered_tables(&sql);
        let (mut recovered, report) =
            SqlEngine::recover_from_committed_kv(kv, tables).expect("recover sql");

        assert_eq!(report.tables, 1);
        assert_eq!(report.row_versions, 3);
        assert_eq!(report.live_rows, 1);
        assert_eq!(report.tombstones, 1);
        assert_eq!(report.highest_version, CommitVersion(6));
        assert_eq!(report.primary_index_entries, 1);
        assert_eq!(report.secondary_index_entries, 1);
        assert_eq!(report.unique_constraint_keys, 1);
        assert_eq!(report.referenced_constraint_keys, 1);
        assert_eq!(
            recovered
                .select_pk("users", "1")
                .expect("recovered row")
                .columns["name"],
            "alan"
        );
        assert!(recovered.select_pk("users", "2").is_none());
        assert_eq!(recovered.select_eq("users", "name", "alan").len(), 1);
        assert!(recovered.select_eq("users", "name", "grace").is_empty());
        assert_eq!(
            recovered
                .select_pk_at("users", "1", CommitVersion(3))
                .expect("historic row")
                .columns["name"],
            "ada"
        );
        assert_eq!(
            recovered
                .select_pk_at("users", "2", CommitVersion(4))
                .expect("historic deleted row")
                .columns["name"],
            "grace"
        );
        assert!(
            recovered
                .select_pk_at("users", "2", CommitVersion(6))
                .is_none()
        );

        let duplicate = recovered.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (3, 'alan')",
        );
        assert!(duplicate.is_err());
    }

    #[test]
    fn sql_surface_commits_through_vsr_backed_router_and_recovers_certificates() {
        let path = test_path("sql_surface_commits_through_vsr_backed_router");
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .expect("router");
            let mut sql = SqlEngine::default();

            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "CREATE TABLE users(id PRIMARY KEY, name)",
            )
            .expect("create table");
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "CREATE INDEX users_name ON users(name)",
            )
            .expect("create index");
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users(id, name) VALUES (1, 'ada')",
            )
            .expect("insert");
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "UPDATE users SET name = 'grace' WHERE id = 1",
            )
            .expect("update");
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "DELETE FROM users WHERE id = 1",
            )
            .expect("delete");

            assert_eq!(router.latest_version(), CommitVersion(5));
            assert_eq!(sql.row_count(), 0);
        }

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover router");

        assert_eq!(recovered.latest_version(), CommitVersion(5));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            5
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn sql_surface_retries_replay_vsr_request_certificates_without_new_commits() {
        let path = test_path("sql_surface_retries_replay_vsr_request_certificates");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut sql = SqlEngine::default();

            run_write_script(&mut sql, &mut router);

            assert_eq!(router.latest_version(), CommitVersion(5));
            assert_eq!(sql.row_count(), 0);
        }

        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let mut retried = SqlEngine::default();

            run_write_script(&mut retried, &mut router);

            assert_eq!(router.latest_version(), CommitVersion(5));
            assert_eq!(retried.row_count(), 0);
        }

        let recovered =
            VsrBackedFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(1), NodeId(0), voters)
                .expect("recover router again");

        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            5
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn unsupported_sql_returns_explicit_error() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        let err = sql
            .execute(&mut router, PlacementGroupId(1), "DROP TABLE users")
            .expect_err("unsupported statement should fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn secondary_index_select_update_and_delete_share_commit_timeline() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = SqlEngine::default();

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
        )
        .expect("create table");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE INDEX users_name ON users(name)",
        )
        .expect("create index");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users(id, name) VALUES (1, 'ada')",
        )
        .expect("insert");

        let SqlOutcome::Selected { rows } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE name = 'ada'",
            )
            .expect("indexed select")
        else {
            panic!("expected selected rows");
        };
        assert_eq!(rows.len(), 1);
        let report = sql.last_query_report().expect("indexed query report");
        assert!(matches!(
            report.physical.operators[0].access,
            AccessPath::SecondaryIndexLookup { ref index, .. } if index == "users_name"
        ));
        assert_eq!(
            report.execution.stages[0].source,
            ExecutionSource::SecondaryIndex
        );

        let SqlOutcome::Updated { row, evidence } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "UPDATE users SET name = 'grace' WHERE id = 1",
            )
            .expect("update")
        else {
            panic!("expected update outcome");
        };
        assert_eq!(row.version, CommitVersion(4));
        assert_eq!(evidence.version(), CommitVersion(4));
        assert!(sql.select_eq("users", "name", "ada").is_empty());
        assert_eq!(sql.select_eq("users", "name", "grace").len(), 1);

        let SqlOutcome::Deleted {
            primary_key,
            evidence,
            ..
        } = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "DELETE FROM users WHERE id = 1",
            )
            .expect("delete")
        else {
            panic!("expected delete outcome");
        };
        assert_eq!(primary_key, "1");
        assert_eq!(evidence.version(), CommitVersion(5));
        assert!(sql.select_eq("users", "name", "grace").is_empty());
        assert_eq!(router.latest_version(), CommitVersion(5));
        assert!(
            sql.encoded_row_at("users", "1", sql.snapshot(&router))
                .is_none()
        );
        assert_eq!(sql.row_count(), 0);
    }

    fn run_write_script(sql: &mut SqlEngine, router: &mut impl FabricRouterPort) {
        let SqlOutcome::TableCreated {
            version, evidence, ..
        } = sql
            .execute(
                router,
                PlacementGroupId(1),
                "CREATE TABLE users(id PRIMARY KEY, name)",
            )
            .expect("create table")
        else {
            panic!("expected create table");
        };
        assert_eq!(version, CommitVersion(1));
        assert_eq!(evidence.version, CommitVersion(1));

        let SqlOutcome::IndexCreated {
            version, evidence, ..
        } = sql
            .execute(
                router,
                PlacementGroupId(1),
                "CREATE INDEX users_name ON users(name)",
            )
            .expect("create index")
        else {
            panic!("expected create index");
        };
        assert_eq!(version, CommitVersion(2));
        assert_eq!(evidence.version, CommitVersion(2));

        let SqlOutcome::Inserted { row, evidence } = sql
            .execute(
                router,
                PlacementGroupId(1),
                "INSERT INTO users(id, name) VALUES (1, 'ada')",
            )
            .expect("insert")
        else {
            panic!("expected insert");
        };
        assert_eq!(row.version, CommitVersion(3));
        assert_eq!(evidence.version(), CommitVersion(3));

        let SqlOutcome::Updated { row, evidence } = sql
            .execute(
                router,
                PlacementGroupId(1),
                "UPDATE users SET name = 'grace' WHERE id = 1",
            )
            .expect("update")
        else {
            panic!("expected update");
        };
        assert_eq!(row.version, CommitVersion(4));
        assert_eq!(evidence.version(), CommitVersion(4));

        let SqlOutcome::Deleted {
            primary_key,
            evidence,
            ..
        } = sql
            .execute(
                router,
                PlacementGroupId(1),
                "DELETE FROM users WHERE id = 1",
            )
            .expect("delete")
        else {
            panic!("expected delete");
        };
        assert_eq!(primary_key, "1");
        assert_eq!(evidence.version(), CommitVersion(5));
    }

    fn recovered_tables(sql: &SqlEngine) -> Vec<SqlRecoveredTable> {
        sql.tables
            .values()
            .map(|table| SqlRecoveredTable {
                descriptor: table.descriptor.clone(),
                profile: table.layout_profile,
                constraints: table
                    .constraints
                    .iter()
                    .filter(|constraint| !matches!(constraint, ConstraintRule::PrimaryKey { .. }))
                    .cloned()
                    .collect(),
                secondary_indexes: table.secondary_indexes.clone(),
            })
            .collect()
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-sql-{name}-{}-{nonce}", std::process::id()))
    }
}
