#![forbid(unsafe_code)]

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WorkloadClass {
    RelationalOltp,
    OrderedKv,
    ObjectMultipart,
    BlobStreaming,
}

impl WorkloadClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RelationalOltp => "relational-oltp",
            Self::OrderedKv => "ordered-kv",
            Self::ObjectMultipart => "object-multipart",
            Self::BlobStreaming => "blob-streaming",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadBudget {
    pub class: WorkloadClass,
    pub p99_budget_ms: u16,
    pub observed_path_ms: u16,
    pub evidence: WorkloadEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadEvidence {
    pub commits: u8,
    pub cache_hits: u8,
    pub provider_choices: u8,
    pub stream_lanes: u8,
    pub regions: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadShapeReport {
    pub budgets: Vec<WorkloadBudget>,
    pub covered_classes: u8,
    pub paths_with_budget: u8,
    pub benchmark_claim: bool,
}

impl WorkloadShapeReport {
    pub fn evaluate(budgets: Vec<WorkloadBudget>) -> Self {
        let covered_classes = budgets
            .iter()
            .filter(|budget| budget.evidence.satisfies(budget.class))
            .count() as u8;
        let paths_with_budget = budgets
            .iter()
            .filter(|budget| budget.observed_path_ms <= budget.p99_budget_ms)
            .count() as u8;

        Self {
            budgets,
            covered_classes,
            paths_with_budget,
            benchmark_claim: false,
        }
    }

    pub fn structural_shape(&self) -> bool {
        self.covered_classes >= 4
            && self.paths_with_budget >= 4
            && !self.benchmark_claim
            && self.budgets.iter().all(|budget| budget.p99_budget_ms > 0)
    }
}

impl WorkloadEvidence {
    fn satisfies(&self, class: WorkloadClass) -> bool {
        match class {
            WorkloadClass::RelationalOltp => self.commits >= 1 && self.cache_hits >= 1,
            WorkloadClass::OrderedKv => self.commits >= 1 && self.regions >= 1,
            WorkloadClass::ObjectMultipart => self.provider_choices >= 1 && self.regions >= 1,
            WorkloadClass::BlobStreaming => self.stream_lanes >= 2 && self.provider_choices >= 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_report_requires_all_database_classes() {
        let report = WorkloadShapeReport::evaluate(vec![
            budget(
                WorkloadClass::RelationalOltp,
                18,
                10,
                evidence(2, 2, 0, 0, 1),
            ),
            budget(WorkloadClass::OrderedKv, 12, 7, evidence(1, 0, 0, 0, 1)),
            budget(
                WorkloadClass::ObjectMultipart,
                45,
                22,
                evidence(0, 0, 2, 0, 2),
            ),
            budget(
                WorkloadClass::BlobStreaming,
                65,
                34,
                evidence(0, 0, 2, 3, 2),
            ),
        ]);

        assert!(report.structural_shape());
        assert_eq!(report.covered_classes, 4);
        assert_eq!(report.paths_with_budget, 4);
        assert!(!report.benchmark_claim);
    }

    #[test]
    fn workload_report_rejects_budget_miss() {
        let report = WorkloadShapeReport::evaluate(vec![
            budget(
                WorkloadClass::RelationalOltp,
                18,
                19,
                evidence(2, 2, 0, 0, 1),
            ),
            budget(WorkloadClass::OrderedKv, 12, 7, evidence(1, 0, 0, 0, 1)),
            budget(
                WorkloadClass::ObjectMultipart,
                45,
                22,
                evidence(0, 0, 2, 0, 2),
            ),
            budget(
                WorkloadClass::BlobStreaming,
                65,
                34,
                evidence(0, 0, 2, 3, 2),
            ),
        ]);

        assert!(!report.structural_shape());
        assert_eq!(report.paths_with_budget, 3);
    }

    fn budget(
        class: WorkloadClass,
        p99_budget_ms: u16,
        observed_path_ms: u16,
        evidence: WorkloadEvidence,
    ) -> WorkloadBudget {
        WorkloadBudget {
            class,
            p99_budget_ms,
            observed_path_ms,
            evidence,
        }
    }

    fn evidence(
        commits: u8,
        cache_hits: u8,
        provider_choices: u8,
        stream_lanes: u8,
        regions: u8,
    ) -> WorkloadEvidence {
        WorkloadEvidence {
            commits,
            cache_hits,
            provider_choices,
            stream_lanes,
            regions,
        }
    }
}
