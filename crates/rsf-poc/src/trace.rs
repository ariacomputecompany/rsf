use std::collections::BTreeSet;
use std::io::Write;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trace {
    events: Vec<LogEvent>,
    echo_stdout: bool,
}

impl Trace {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            echo_stdout: false,
        }
    }

    pub fn streaming_stdout() -> Self {
        Self {
            events: Vec::new(),
            echo_stdout: true,
        }
    }

    pub fn push(
        &mut self,
        service: impl Into<String>,
        action: impl Into<String>,
        detail: impl Into<String>,
    ) {
        self.events.push(LogEvent {
            seq: self.events.len() as u64 + 1,
            service: service.into(),
            action: action.into(),
            detail: detail.into(),
        });
        if self.echo_stdout {
            if let Some(event) = self.events.last() {
                let mut stdout = std::io::stdout().lock();
                let _ = writeln!(stdout, "{}", render_event(event));
                let _ = stdout.flush();
            }
        }
    }

    pub fn events(&self) -> &[LogEvent] {
        &self.events
    }

    pub fn render(&self) -> String {
        self.events
            .iter()
            .map(render_event)
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn services(&self) -> Vec<&str> {
        self.events
            .iter()
            .map(|event| event.service.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn render_service(&self, service: &str) -> String {
        self.events
            .iter()
            .filter(|event| event.service == service)
            .map(render_event)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Default for Trace {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogEvent {
    pub seq: u64,
    pub service: String,
    pub action: String,
    pub detail: String,
}

fn render_event(event: &LogEvent) -> String {
    format!(
        "[{:03}] {:<18} {:<22} {}",
        event.seq, event.service, event.action, event.detail
    )
}
