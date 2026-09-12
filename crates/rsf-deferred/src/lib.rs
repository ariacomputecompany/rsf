#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use rsf_core::{ErrorCode, RsfError, RsfResult, catalog::TableName, transaction::TransactionId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredReferenceRule {
    pub name: String,
    pub table: TableName,
    pub columns: Vec<String>,
    pub referenced_table: TableName,
    pub referenced_columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredRow {
    pub table: TableName,
    pub values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CommittedReferences {
    keys: BTreeMap<String, BTreeSet<Vec<String>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeferredConstraintDecision {
    Began {
        transaction: TransactionId,
    },
    ReferencedKeyStaged {
        transaction: TransactionId,
        reference: String,
        key: Vec<String>,
    },
    ObligationStaged {
        transaction: TransactionId,
        rule: String,
        key: Vec<String>,
    },
    CommitAccepted {
        transaction: TransactionId,
        obligations: usize,
        transaction_references: usize,
        committed_references: usize,
    },
    CommitRejected {
        transaction: TransactionId,
        violations: Vec<DeferredConstraintViolation>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredConstraintViolation {
    pub rule: String,
    pub reference: String,
    pub key: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeferredConstraintReport {
    pub transactions: usize,
    pub obligations: usize,
    pub transaction_references: usize,
    pub committed_references: usize,
    pub commits: usize,
    pub rejections: usize,
}

#[derive(Clone, Debug, Default)]
pub struct DeferredConstraintEngine {
    active: BTreeMap<TransactionId, DeferredTransaction>,
    transactions: usize,
    obligations: usize,
    transaction_references: usize,
    committed_references: usize,
    commits: usize,
    rejections: usize,
}

#[derive(Clone, Debug, Default)]
struct DeferredTransaction {
    obligations: Vec<DeferredObligation>,
    references: BTreeMap<String, BTreeSet<Vec<String>>>,
}

#[derive(Clone, Debug)]
struct DeferredObligation {
    rule: String,
    reference: String,
    key: Vec<String>,
}

impl DeferredConstraintEngine {
    pub fn begin(&mut self, transaction: TransactionId) -> RsfResult<DeferredConstraintDecision> {
        if self.active.contains_key(&transaction) {
            return self.reject(format!(
                "deferred constraint transaction {} is already active",
                txn_label(&transaction)
            ));
        }
        self.active
            .insert(transaction.clone(), DeferredTransaction::default());
        self.transactions += 1;
        Ok(DeferredConstraintDecision::Began { transaction })
    }

    pub fn stage_referenced_key(
        &mut self,
        transaction: &TransactionId,
        table: &TableName,
        columns: &[String],
        key: impl IntoIterator<Item = impl Into<String>>,
    ) -> RsfResult<DeferredConstraintDecision> {
        validate_table_name(table)?;
        validate_columns(columns)?;
        let Some(active) = self.active.get_mut(transaction) else {
            return self.reject("deferred constraint transaction is not active");
        };
        let key = collect_key(key)?;
        let reference = reference_name(table, columns);
        active
            .references
            .entry(reference.clone())
            .or_default()
            .insert(key.clone());
        self.transaction_references += 1;
        Ok(DeferredConstraintDecision::ReferencedKeyStaged {
            transaction: transaction.clone(),
            reference,
            key,
        })
    }

    pub fn defer_foreign_key(
        &mut self,
        transaction: &TransactionId,
        rule: DeferredReferenceRule,
        row: &DeferredRow,
    ) -> RsfResult<DeferredConstraintDecision> {
        validate_rule(&rule)?;
        if row.table != rule.table {
            return Err(invalid(format!(
                "deferred row targets {}, not rule table {}",
                row.table.0, rule.table.0
            )));
        }
        let Some(active) = self.active.get_mut(transaction) else {
            return self.reject("deferred constraint transaction is not active");
        };
        let key = key_for(&rule.columns, row)?;
        let obligation = DeferredObligation {
            rule: rule.name.clone(),
            reference: reference_name(&rule.referenced_table, &rule.referenced_columns),
            key: key.clone(),
        };
        active.obligations.push(obligation);
        self.obligations += 1;
        Ok(DeferredConstraintDecision::ObligationStaged {
            transaction: transaction.clone(),
            rule: rule.name,
            key,
        })
    }

    pub fn commit(
        &mut self,
        transaction: &TransactionId,
        committed: &CommittedReferences,
    ) -> RsfResult<DeferredConstraintDecision> {
        let Some(active) = self.active.remove(transaction) else {
            return self.reject("deferred constraint transaction is not active");
        };
        let mut violations = Vec::new();
        for obligation in &active.obligations {
            let exists_in_transaction = active
                .references
                .get(&obligation.reference)
                .is_some_and(|known| known.contains(&obligation.key));
            let exists_committed = committed.contains(&obligation.reference, &obligation.key);
            if !exists_in_transaction && !exists_committed {
                violations.push(DeferredConstraintViolation {
                    rule: obligation.rule.clone(),
                    reference: obligation.reference.clone(),
                    key: obligation.key.clone(),
                });
            }
        }

        if !violations.is_empty() {
            self.rejections += 1;
            return Ok(DeferredConstraintDecision::CommitRejected {
                transaction: transaction.clone(),
                violations,
            });
        }

        self.committed_references += committed.key_count();
        self.commits += 1;
        Ok(DeferredConstraintDecision::CommitAccepted {
            transaction: transaction.clone(),
            obligations: active.obligations.len(),
            transaction_references: active.references.values().map(BTreeSet::len).sum(),
            committed_references: committed.key_count(),
        })
    }

    pub fn report(&self) -> DeferredConstraintReport {
        DeferredConstraintReport {
            transactions: self.transactions,
            obligations: self.obligations,
            transaction_references: self.transaction_references,
            committed_references: self.committed_references,
            commits: self.commits,
            rejections: self.rejections,
        }
    }

    fn reject<T>(&mut self, message: impl Into<String>) -> RsfResult<T> {
        self.rejections += 1;
        Err(invalid(message))
    }
}

impl CommittedReferences {
    pub fn add(
        &mut self,
        table: &TableName,
        columns: &[String],
        key: impl IntoIterator<Item = impl Into<String>>,
    ) -> RsfResult<()> {
        validate_table_name(table)?;
        validate_columns(columns)?;
        let key = collect_key(key)?;
        self.keys
            .entry(reference_name(table, columns))
            .or_default()
            .insert(key);
        Ok(())
    }

    pub fn add_reference(
        &mut self,
        reference: impl Into<String>,
        key: impl IntoIterator<Item = impl Into<String>>,
    ) -> RsfResult<()> {
        let reference = reference.into();
        validate_reference_name(&reference)?;
        let key = collect_key(key)?;
        self.keys.entry(reference).or_default().insert(key);
        Ok(())
    }

    pub fn remove(
        &mut self,
        table: &TableName,
        columns: &[String],
        key: impl IntoIterator<Item = impl Into<String>>,
    ) -> RsfResult<()> {
        validate_table_name(table)?;
        validate_columns(columns)?;
        self.remove_reference(reference_name(table, columns), key)
    }

    pub fn remove_reference(
        &mut self,
        reference: impl AsRef<str>,
        key: impl IntoIterator<Item = impl Into<String>>,
    ) -> RsfResult<()> {
        validate_reference_name(reference.as_ref())?;
        let key = collect_key(key)?;
        if let Some(keys) = self.keys.get_mut(reference.as_ref()) {
            keys.remove(&key);
            if keys.is_empty() {
                self.keys.remove(reference.as_ref());
            }
        }
        Ok(())
    }

    pub fn key_count(&self) -> usize {
        self.keys.values().map(BTreeSet::len).sum()
    }

    fn contains(&self, reference: &str, key: &[String]) -> bool {
        self.keys
            .get(reference)
            .is_some_and(|known| known.contains(key))
    }
}

fn validate_rule(rule: &DeferredReferenceRule) -> RsfResult<()> {
    if rule.name.trim().is_empty() {
        return Err(invalid("deferred constraint rule name must not be empty"));
    }
    validate_table_name(&rule.table)?;
    validate_table_name(&rule.referenced_table)?;
    validate_columns(&rule.columns)?;
    validate_columns(&rule.referenced_columns)?;
    if rule.columns.len() != rule.referenced_columns.len() {
        return Err(invalid("deferred reference column counts must match"));
    }
    Ok(())
}

fn validate_table_name(table: &TableName) -> RsfResult<()> {
    if table.0.trim().is_empty() {
        Err(invalid("deferred constraint table name must not be empty"))
    } else if table.0 != table.0.trim() {
        Err(invalid(
            "deferred constraint table name must not contain edge whitespace",
        ))
    } else if table.0.contains(['(', ')', ',']) {
        Err(invalid(
            "deferred constraint table name contains reference delimiter characters",
        ))
    } else {
        Ok(())
    }
}

fn validate_columns(columns: &[String]) -> RsfResult<()> {
    if columns.is_empty() {
        return Err(invalid("deferred constraint key must include columns"));
    }
    let mut seen = BTreeSet::new();
    for column in columns {
        if column.trim().is_empty() {
            return Err(invalid("deferred constraint columns must not be empty"));
        }
        if column.as_str() != column.trim() {
            return Err(invalid(
                "deferred constraint columns must not contain edge whitespace",
            ));
        }
        if !seen.insert(column.as_str()) {
            return Err(invalid(format!(
                "deferred constraint column {column} is duplicated"
            )));
        }
    }
    Ok(())
}

fn validate_reference_name(reference: &str) -> RsfResult<()> {
    let Some((table, columns)) = reference.split_once('(') else {
        return Err(invalid(
            "deferred reference name must use table(column,...) format",
        ));
    };
    if !columns.ends_with(')') {
        return Err(invalid(
            "deferred reference name must use table(column,...) format",
        ));
    }
    if columns[..columns.len() - 1].contains(['(', ')']) {
        return Err(invalid(
            "deferred reference columns contain reference delimiter characters",
        ));
    }
    validate_table_name(&TableName(table.to_string()))?;
    let columns = columns[..columns.len() - 1]
        .split(',')
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    validate_columns(&columns)
}

fn key_for(columns: &[String], row: &DeferredRow) -> RsfResult<Vec<String>> {
    columns
        .iter()
        .map(|column| {
            row.values
                .get(column)
                .cloned()
                .ok_or_else(|| invalid(format!("deferred row is missing column {column}")))
        })
        .collect()
}

fn collect_key(key: impl IntoIterator<Item = impl Into<String>>) -> RsfResult<Vec<String>> {
    let key = key.into_iter().map(Into::into).collect::<Vec<_>>();
    if key.is_empty() || key.iter().any(|part| part.trim().is_empty()) {
        Err(invalid("deferred constraint key must not be empty"))
    } else {
        Ok(key)
    }
}

fn reference_name(table: &TableName, columns: &[String]) -> String {
    format!("{}({})", table.0, columns.join(","))
}

fn txn_label(transaction: &TransactionId) -> String {
    transaction
        .0
        .iter()
        .take(4)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn txn(value: u8) -> TransactionId {
        TransactionId([value; 16])
    }

    fn txn_with_prefix(prefix: [u8; 4], suffix: u8) -> TransactionId {
        let mut bytes = [suffix; 16];
        bytes[..4].copy_from_slice(&prefix);
        TransactionId(bytes)
    }

    fn rule() -> DeferredReferenceRule {
        DeferredReferenceRule {
            name: "orders_user".to_string(),
            table: TableName("orders".to_string()),
            columns: vec!["user_id".to_string()],
            referenced_table: TableName("users".to_string()),
            referenced_columns: vec!["id".to_string()],
        }
    }

    fn order(user_id: &str) -> DeferredRow {
        DeferredRow {
            table: TableName("orders".to_string()),
            values: BTreeMap::from([
                ("id".to_string(), "900".to_string()),
                ("user_id".to_string(), user_id.to_string()),
            ]),
        }
    }

    #[test]
    fn transaction_local_reference_satisfies_deferred_foreign_key() {
        let mut engine = DeferredConstraintEngine::default();
        let transaction = txn(1);
        engine.begin(transaction.clone()).unwrap();
        engine
            .defer_foreign_key(&transaction, rule(), &order("42"))
            .unwrap();
        engine
            .stage_referenced_key(
                &transaction,
                &TableName("users".to_string()),
                &["id".to_string()],
                ["42"],
            )
            .unwrap();

        let decision = engine
            .commit(&transaction, &CommittedReferences::default())
            .unwrap();

        assert!(matches!(
            decision,
            DeferredConstraintDecision::CommitAccepted {
                obligations: 1,
                transaction_references: 1,
                ..
            }
        ));
        assert_eq!(engine.report().commits, 1);
        assert_eq!(engine.report().rejections, 0);
    }

    #[test]
    fn committed_reference_satisfies_deferred_foreign_key() {
        let mut engine = DeferredConstraintEngine::default();
        let transaction = txn(2);
        let mut committed = CommittedReferences::default();
        committed
            .add(&TableName("users".to_string()), &["id".to_string()], ["42"])
            .unwrap();
        engine.begin(transaction.clone()).unwrap();
        engine
            .defer_foreign_key(&transaction, rule(), &order("42"))
            .unwrap();

        let decision = engine.commit(&transaction, &committed).unwrap();

        assert!(matches!(
            decision,
            DeferredConstraintDecision::CommitAccepted {
                committed_references: 1,
                ..
            }
        ));
    }

    #[test]
    fn missing_reference_rejects_at_commit_time() {
        let mut engine = DeferredConstraintEngine::default();
        let transaction = txn(3);
        engine.begin(transaction.clone()).unwrap();
        engine
            .defer_foreign_key(&transaction, rule(), &order("missing"))
            .unwrap();

        let decision = engine
            .commit(&transaction, &CommittedReferences::default())
            .unwrap();

        let DeferredConstraintDecision::CommitRejected { violations, .. } = decision else {
            panic!("missing deferred reference should reject commit");
        };
        assert_eq!(violations[0].rule, "orders_user");
        assert_eq!(violations[0].key, vec!["missing".to_string()]);
        assert_eq!(engine.report().rejections, 1);
    }

    #[test]
    fn duplicate_rule_columns_are_rejected() {
        let mut engine = DeferredConstraintEngine::default();
        let transaction = txn(4);
        engine.begin(transaction.clone()).unwrap();
        let mut rule = rule();
        rule.columns = vec!["user_id".to_string(), "user_id".to_string()];
        rule.referenced_columns = vec!["id".to_string(), "id2".to_string()];

        let err = engine
            .defer_foreign_key(&transaction, rule, &order("42"))
            .expect_err("duplicate deferred rule columns must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("duplicated"));
    }

    #[test]
    fn committed_references_reject_duplicate_columns() {
        let mut committed = CommittedReferences::default();

        let err = committed
            .add(
                &TableName("users".to_string()),
                &["id".to_string(), "id".to_string()],
                ["42", "42"],
            )
            .expect_err("duplicate reference columns must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("duplicated"));
    }

    #[test]
    fn raw_committed_reference_must_be_canonical() {
        let mut committed = CommittedReferences::default();

        let err = committed
            .add_reference("users", ["42"])
            .expect_err("raw reference without columns must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("table(column,...) format"));
    }

    #[test]
    fn transactions_with_same_display_prefix_remain_distinct() {
        let mut engine = DeferredConstraintEngine::default();
        let first = txn_with_prefix([1, 2, 3, 4], 0xaa);
        let second = txn_with_prefix([1, 2, 3, 4], 0xbb);

        engine.begin(first.clone()).unwrap();
        engine.begin(second.clone()).unwrap();
        engine
            .defer_foreign_key(&first, rule(), &order("42"))
            .unwrap();
        engine
            .stage_referenced_key(
                &second,
                &TableName("users".to_string()),
                &["id".to_string()],
                ["42"],
            )
            .unwrap();

        let rejected = engine
            .commit(&first, &CommittedReferences::default())
            .unwrap();
        assert!(matches!(
            rejected,
            DeferredConstraintDecision::CommitRejected { .. }
        ));

        let accepted = engine
            .commit(&second, &CommittedReferences::default())
            .unwrap();
        assert!(matches!(
            accepted,
            DeferredConstraintDecision::CommitAccepted {
                obligations: 0,
                transaction_references: 1,
                ..
            }
        ));
    }
}
