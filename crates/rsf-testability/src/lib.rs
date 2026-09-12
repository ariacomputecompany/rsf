#![forbid(unsafe_code)]

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestabilityInput {
    pub trace_events: u16,
    pub trace_services: u8,
    pub invariant_failed: u8,
    pub observability_metrics: u16,
    pub observability_audits: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestabilityReport {
    pub checks: Vec<TestabilityCheck>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestabilityCheck {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

impl TestabilityReport {
    pub fn evaluate(input: &TestabilityInput) -> Self {
        Self {
            checks: vec![
                check(
                    "trace-depth",
                    input.trace_events >= 600,
                    format!("trace_events={}", input.trace_events),
                ),
                check(
                    "service-coverage",
                    input.trace_services >= 60,
                    format!("trace_services={}", input.trace_services),
                ),
                check(
                    "invariant-clean",
                    input.invariant_failed == 0,
                    format!("invariant_failed={}", input.invariant_failed),
                ),
                check(
                    "metrics-rich",
                    input.observability_metrics >= 300,
                    format!("observability_metrics={}", input.observability_metrics),
                ),
                check(
                    "audit-present",
                    input.observability_audits >= 2,
                    format!("observability_audits={}", input.observability_audits),
                ),
            ],
        }
    }

    pub fn passed(&self) -> usize {
        self.checks.iter().filter(|check| check.passed).count()
    }

    pub fn total(&self) -> usize {
        self.checks.len()
    }

    pub fn replay_ready(&self) -> bool {
        self.passed() == self.total()
    }
}

fn check(name: &'static str, passed: bool, detail: String) -> TestabilityCheck {
    TestabilityCheck {
        name,
        passed,
        detail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_production_trace_is_replay_ready() {
        let report = TestabilityReport::evaluate(&TestabilityInput {
            trace_events: 700,
            trace_services: 70,
            invariant_failed: 0,
            observability_metrics: 301,
            observability_audits: 2,
        });

        assert!(report.replay_ready());
        assert_eq!(report.passed(), 5);
    }

    #[test]
    fn missing_metrics_blocks_replay_ready() {
        let report = TestabilityReport::evaluate(&TestabilityInput {
            trace_events: 700,
            trace_services: 70,
            invariant_failed: 0,
            observability_metrics: 12,
            observability_audits: 2,
        });

        assert!(!report.replay_ready());
        assert_eq!(report.passed(), 4);
    }
}
