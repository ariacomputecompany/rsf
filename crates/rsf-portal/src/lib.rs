#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_core::{CommitVersion, RsfResult};
use rsf_prepared::{
    BoundPreparedSql, PreparedCache, PreparedDecision, PreparedParamType, PreparedValue,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Portal {
    pub name: String,
    pub statement: String,
    pub bound: BoundPreparedSql,
    pub described: bool,
    pub executed: bool,
    pub cancelled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PortalDecision {
    Parsed {
        statement: String,
        parameters: usize,
        schema_version: CommitVersion,
    },
    Bound {
        portal: String,
        statement: String,
        parameters: usize,
    },
    Described {
        portal: String,
        columns: usize,
        parameters: usize,
    },
    Executed {
        portal: String,
        rows: usize,
        elapsed_ms: u64,
    },
    Cancelled {
        portal: String,
        reason: String,
    },
    ClosedPortal {
        portal: String,
    },
    ClosedStatement {
        statement: String,
    },
    Rejected {
        target: String,
        reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct SqlPortalSession {
    cache: PreparedCache,
    portals: BTreeMap<String, Portal>,
}

impl Default for SqlPortalSession {
    fn default() -> Self {
        Self::new(128).expect("default portal session capacity is valid")
    }
}

impl SqlPortalSession {
    pub fn new(statement_capacity: usize) -> RsfResult<Self> {
        Ok(Self {
            cache: PreparedCache::new(statement_capacity)?,
            portals: BTreeMap::new(),
        })
    }

    pub fn parse(
        &mut self,
        statement: &str,
        sql: &str,
        schema_version: CommitVersion,
        parameter_types: Vec<PreparedParamType>,
    ) -> Vec<PortalDecision> {
        match self
            .cache
            .prepare(statement, sql, schema_version, parameter_types)
        {
            Ok(decisions) => decisions
                .into_iter()
                .map(prepared_to_portal_decision)
                .collect(),
            Err(error) => vec![rejected(statement, error.message)],
        }
    }

    pub fn bind(
        &mut self,
        portal: &str,
        statement: &str,
        current_schema_version: CommitVersion,
        parameters: Vec<PreparedValue>,
    ) -> PortalDecision {
        if portal.trim().is_empty() {
            return rejected("portal", "portal name must not be empty");
        }
        match self
            .cache
            .bind(statement, current_schema_version, parameters)
        {
            Ok(PreparedDecision::Bound(bound)) => {
                let parameter_count = bound.parameters.len();
                self.portals.insert(
                    portal.to_string(),
                    Portal {
                        name: portal.to_string(),
                        statement: statement.to_string(),
                        bound,
                        described: false,
                        executed: false,
                        cancelled: false,
                    },
                );
                PortalDecision::Bound {
                    portal: portal.to_string(),
                    statement: statement.to_string(),
                    parameters: parameter_count,
                }
            }
            Ok(other) => prepared_to_portal_decision(other),
            Err(error) => rejected(statement, error.message),
        }
    }

    pub fn describe(&mut self, portal: &str) -> PortalDecision {
        let Some(portal_state) = self.portals.get_mut(portal) else {
            return rejected(portal, "portal is not bound");
        };
        if portal_state.cancelled {
            return rejected(portal, "portal is cancelled");
        }
        portal_state.described = true;
        PortalDecision::Described {
            portal: portal.to_string(),
            columns: projected_columns(&portal_state.bound.sql),
            parameters: portal_state.bound.parameters.len(),
        }
    }

    pub fn execute(
        &mut self,
        portal: &str,
        deadline_ms: u64,
        elapsed_ms: u64,
        rows: usize,
    ) -> PortalDecision {
        self.mark_executed(portal, deadline_ms, elapsed_ms, rows)
    }

    pub fn bound_sql(&self, portal: &str) -> RsfResult<String> {
        let Some(portal_state) = self.portals.get(portal) else {
            return Err(rsf_core::RsfError::new(
                rsf_core::ErrorCode::InvalidInput,
                "portal is not bound",
            ));
        };
        if portal_state.cancelled {
            return Err(rsf_core::RsfError::new(
                rsf_core::ErrorCode::InvalidInput,
                "portal is cancelled",
            ));
        }
        Ok(portal_state.bound.sql.clone())
    }

    pub fn mark_executed(
        &mut self,
        portal: &str,
        deadline_ms: u64,
        elapsed_ms: u64,
        rows: usize,
    ) -> PortalDecision {
        let Some(portal_state) = self.portals.get_mut(portal) else {
            return rejected(portal, "portal is not bound");
        };
        if portal_state.cancelled {
            return rejected(portal, "portal is cancelled");
        }
        if deadline_ms > 0 && elapsed_ms > deadline_ms {
            return rejected(
                portal,
                format!("deadline exceeded: elapsed_ms={elapsed_ms} deadline_ms={deadline_ms}"),
            );
        }
        portal_state.executed = true;
        PortalDecision::Executed {
            portal: portal.to_string(),
            rows,
            elapsed_ms,
        }
    }

    pub fn cancel(&mut self, portal: &str, reason: impl Into<String>) -> PortalDecision {
        let Some(portal_state) = self.portals.get_mut(portal) else {
            return rejected(portal, "portal is not bound");
        };
        portal_state.cancelled = true;
        PortalDecision::Cancelled {
            portal: portal.to_string(),
            reason: reason.into(),
        }
    }

    pub fn close_portal(&mut self, portal: &str) -> PortalDecision {
        if self.portals.remove(portal).is_some() {
            PortalDecision::ClosedPortal {
                portal: portal.to_string(),
            }
        } else {
            rejected(portal, "portal is not bound")
        }
    }

    pub fn close_statement(&mut self, statement: &str) -> PortalDecision {
        self.portals
            .retain(|_, portal| portal.statement != statement);
        match self.cache.deallocate(statement) {
            Ok(PreparedDecision::Deallocated { name }) => {
                PortalDecision::ClosedStatement { statement: name }
            }
            Ok(other) => prepared_to_portal_decision(other),
            Err(error) => rejected(statement, error.message),
        }
    }

    pub fn portal_count(&self) -> usize {
        self.portals.len()
    }
}

fn prepared_to_portal_decision(decision: PreparedDecision) -> PortalDecision {
    match decision {
        PreparedDecision::Prepared {
            name,
            parameters,
            schema_version,
        } => PortalDecision::Parsed {
            statement: name,
            parameters,
            schema_version,
        },
        PreparedDecision::Bound(bound) => PortalDecision::Bound {
            portal: bound.name.clone(),
            statement: bound.name,
            parameters: bound.parameters.len(),
        },
        PreparedDecision::Invalidated {
            name,
            prepared_version,
            current_version,
        } => rejected(
            name,
            format!(
                "statement invalidated: prepared_version={} current_version={}",
                prepared_version.0, current_version.0
            ),
        ),
        PreparedDecision::Deallocated { name } => {
            PortalDecision::ClosedStatement { statement: name }
        }
        PreparedDecision::Evicted { name } => {
            rejected(name, "statement evicted before portal bind")
        }
        PreparedDecision::Rejected { name, reason } => rejected(name, reason),
    }
}

fn projected_columns(sql: &str) -> usize {
    let Some(select_tail) = sql.trim().strip_prefix("SELECT ") else {
        return 0;
    };
    let Some((projection, _)) = select_tail.split_once(" FROM ") else {
        return 0;
    };
    projection
        .split(',')
        .filter(|column| !column.trim().is_empty())
        .count()
}

fn rejected(target: impl Into<String>, reason: impl Into<String>) -> PortalDecision {
    PortalDecision::Rejected {
        target: target.into(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bind_describe_execute_and_close_portal() {
        let mut session = SqlPortalSession::new(4).unwrap();

        assert_eq!(
            session.parse(
                "select_user",
                "SELECT id, name FROM users WHERE id = $1",
                CommitVersion(7),
                vec![PreparedParamType::Int64],
            ),
            vec![PortalDecision::Parsed {
                statement: "select_user".to_string(),
                parameters: 1,
                schema_version: CommitVersion(7),
            }]
        );
        assert!(matches!(
            session.bind(
                "portal_1",
                "select_user",
                CommitVersion(7),
                vec![PreparedValue::Int64(42)],
            ),
            PortalDecision::Bound { .. }
        ));
        assert_eq!(
            session.describe("portal_1"),
            PortalDecision::Described {
                portal: "portal_1".to_string(),
                columns: 2,
                parameters: 1,
            }
        );
        assert_eq!(
            session.execute("portal_1", 100, 15, 1),
            PortalDecision::Executed {
                portal: "portal_1".to_string(),
                rows: 1,
                elapsed_ms: 15,
            }
        );
        assert_eq!(
            session.bound_sql("portal_1").unwrap(),
            "SELECT id, name FROM users WHERE id = 42"
        );
        assert_eq!(
            session.close_portal("portal_1"),
            PortalDecision::ClosedPortal {
                portal: "portal_1".to_string(),
            }
        );
    }

    #[test]
    fn cancelled_portal_rejects_execution() {
        let mut session = prepared_session();
        assert!(matches!(
            session.cancel("portal_1", "client cancel"),
            PortalDecision::Cancelled { .. }
        ));
        assert!(matches!(
            session.execute("portal_1", 100, 10, 1),
            PortalDecision::Rejected { .. }
        ));
    }

    #[test]
    fn deadline_rejects_slow_execution() {
        let mut session = prepared_session();
        assert_eq!(
            session.execute("portal_1", 10, 11, 1),
            PortalDecision::Rejected {
                target: "portal_1".to_string(),
                reason: "deadline exceeded: elapsed_ms=11 deadline_ms=10".to_string(),
            }
        );
    }

    #[test]
    fn close_statement_removes_bound_portals() {
        let mut session = prepared_session();
        assert_eq!(session.portal_count(), 1);
        assert_eq!(
            session.close_statement("select_user"),
            PortalDecision::ClosedStatement {
                statement: "select_user".to_string(),
            }
        );
        assert_eq!(session.portal_count(), 0);
    }

    fn prepared_session() -> SqlPortalSession {
        let mut session = SqlPortalSession::new(4).unwrap();
        session.parse(
            "select_user",
            "SELECT id, name FROM users WHERE id = $1",
            CommitVersion(7),
            vec![PreparedParamType::Int64],
        );
        session.bind(
            "portal_1",
            "select_user",
            CommitVersion(7),
            vec![PreparedValue::Int64(42)],
        );
        session
    }
}
