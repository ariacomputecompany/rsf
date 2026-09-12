#![forbid(unsafe_code)]

use rsf_core::{ErrorCode, RsfError, RsfResult};
use rsf_planner::{AccessPath, PhysicalPlan};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionReport {
    pub stages: Vec<ExecutionStage>,
    pub rows: u64,
    pub bytes: u64,
    pub remote_reads: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionStage {
    pub source: ExecutionSource,
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionSource {
    Cache,
    PrimaryIndex,
    SecondaryIndex,
    Range,
    Stream,
}

#[derive(Clone, Debug, Default)]
pub struct PhysicalExecutor;

impl PhysicalExecutor {
    pub fn execute(&self, plan: &PhysicalPlan) -> RsfResult<ExecutionReport> {
        if plan.operators.is_empty() {
            return Err(invalid("physical plan must include at least one operator"));
        }

        let mut stages = Vec::new();
        let mut remote_reads = 0;
        let mut saw_data_access = false;
        let mut saw_stream = false;
        for operator in &plan.operators {
            if saw_stream {
                return Err(invalid("stream read must be the final physical operator"));
            }
            let source = match &operator.access {
                AccessPath::CacheLookup => {
                    if saw_data_access {
                        return Err(invalid("cache lookup must precede data access"));
                    }
                    ExecutionSource::Cache
                }
                AccessPath::PrimaryKeyLookup { .. } => {
                    mark_data_access(&mut saw_data_access)?;
                    ExecutionSource::PrimaryIndex
                }
                AccessPath::SecondaryIndexLookup { .. } => {
                    mark_data_access(&mut saw_data_access)?;
                    ExecutionSource::SecondaryIndex
                }
                AccessPath::RangeScan => {
                    mark_data_access(&mut saw_data_access)?;
                    ExecutionSource::Range
                }
                AccessPath::StreamRead { threshold_bytes } => {
                    if *threshold_bytes == 0 {
                        return Err(invalid("stream read threshold must be greater than zero"));
                    }
                    if !saw_data_access {
                        return Err(invalid("stream read requires a preceding data access"));
                    }
                    saw_stream = true;
                    ExecutionSource::Stream
                }
            };
            if source != ExecutionSource::Cache {
                remote_reads += 1;
            }
            stages.push(ExecutionStage {
                source,
                rows: operator.estimated_rows,
                bytes: operator.estimated_bytes,
            });
        }

        Ok(ExecutionReport {
            stages,
            rows: plan.estimated_rows,
            bytes: plan.estimated_bytes,
            remote_reads,
        })
    }
}

fn mark_data_access(saw_data_access: &mut bool) -> RsfResult<()> {
    if *saw_data_access {
        return Err(invalid(
            "physical plan must include only one data access operator",
        ));
    }
    *saw_data_access = true;
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use rsf_core::catalog::TableName;
    use rsf_planner::{AccessPath, PhysicalOperator};

    use super::*;

    fn plan(operators: Vec<PhysicalOperator>) -> PhysicalPlan {
        PhysicalPlan {
            table: TableName("users".to_string()),
            operators,
            estimated_rows: 5,
            estimated_bytes: 1_280,
        }
    }

    #[test]
    fn executes_cache_then_secondary_index_plan() {
        let report = PhysicalExecutor
            .execute(&plan(vec![
                PhysicalOperator {
                    access: AccessPath::CacheLookup,
                    estimated_rows: 5,
                    estimated_bytes: 2_560,
                },
                PhysicalOperator {
                    access: AccessPath::SecondaryIndexLookup {
                        index: "users_name".to_string(),
                        column: "name".to_string(),
                    },
                    estimated_rows: 5,
                    estimated_bytes: 2_560,
                },
            ]))
            .unwrap();

        assert_eq!(
            report
                .stages
                .iter()
                .map(|stage| stage.source.clone())
                .collect::<Vec<_>>(),
            vec![ExecutionSource::Cache, ExecutionSource::SecondaryIndex]
        );
        assert_eq!(report.remote_reads, 1);
        assert_eq!(report.rows, 5);
    }

    #[test]
    fn executes_range_then_stream_plan() {
        let report = PhysicalExecutor
            .execute(&plan(vec![
                PhysicalOperator {
                    access: AccessPath::RangeScan,
                    estimated_rows: 64,
                    estimated_bytes: 32_768,
                },
                PhysicalOperator {
                    access: AccessPath::StreamRead {
                        threshold_bytes: 8_192,
                    },
                    estimated_rows: 64,
                    estimated_bytes: 32_768,
                },
            ]))
            .unwrap();

        assert_eq!(report.remote_reads, 2);
        assert_eq!(report.stages[0].source, ExecutionSource::Range);
        assert_eq!(report.stages[1].source, ExecutionSource::Stream);
    }

    #[test]
    fn empty_plan_is_rejected() {
        let err = PhysicalExecutor
            .execute(&plan(Vec::new()))
            .expect_err("empty plan must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn stream_without_data_access_is_rejected() {
        let err = PhysicalExecutor
            .execute(&plan(vec![PhysicalOperator {
                access: AccessPath::StreamRead {
                    threshold_bytes: 8_192,
                },
                estimated_rows: 64,
                estimated_bytes: 32_768,
            }]))
            .expect_err("stream requires a preceding data access");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("preceding data access"));
    }

    #[test]
    fn cache_after_data_access_is_rejected() {
        let err = PhysicalExecutor
            .execute(&plan(vec![
                PhysicalOperator {
                    access: AccessPath::RangeScan,
                    estimated_rows: 64,
                    estimated_bytes: 32_768,
                },
                PhysicalOperator {
                    access: AccessPath::CacheLookup,
                    estimated_rows: 64,
                    estimated_bytes: 32_768,
                },
            ]))
            .expect_err("cache cannot follow data access");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("must precede data access"));
    }

    #[test]
    fn multiple_data_access_operators_are_rejected() {
        let err = PhysicalExecutor
            .execute(&plan(vec![
                PhysicalOperator {
                    access: AccessPath::RangeScan,
                    estimated_rows: 64,
                    estimated_bytes: 32_768,
                },
                PhysicalOperator {
                    access: AccessPath::PrimaryKeyLookup {
                        column: "id".to_string(),
                    },
                    estimated_rows: 1,
                    estimated_bytes: 128,
                },
            ]))
            .expect_err("plan cannot contain competing data access operators");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("only one data access"));
    }

    #[test]
    fn operator_after_stream_is_rejected() {
        let err = PhysicalExecutor
            .execute(&plan(vec![
                PhysicalOperator {
                    access: AccessPath::RangeScan,
                    estimated_rows: 64,
                    estimated_bytes: 32_768,
                },
                PhysicalOperator {
                    access: AccessPath::StreamRead {
                        threshold_bytes: 8_192,
                    },
                    estimated_rows: 64,
                    estimated_bytes: 32_768,
                },
                PhysicalOperator {
                    access: AccessPath::CacheLookup,
                    estimated_rows: 64,
                    estimated_bytes: 32_768,
                },
            ]))
            .expect_err("stream must terminate execution");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("final physical operator"));
    }
}
