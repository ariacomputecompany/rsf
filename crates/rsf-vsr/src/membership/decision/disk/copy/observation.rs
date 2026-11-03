//! Crate-private caller boundary faults, not simulated OS fsync failures.
use super::*;
use std::cell::RefCell;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::membership) enum Boundary {
    Closed,
    Active,
    Complete,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::membership) struct Event {
    pub boundary: Boundary,
    pub retained: usize,
    pub total: usize,
}
struct Observation {
    fail_at: Option<usize>,
    events: Vec<Event>,
}
thread_local! { static STATE: RefCell<Option<Observation>> = const { RefCell::new(None) }; }
pub(in crate::membership) struct Guard;
impl Guard {
    pub fn events(&self) -> Vec<Event> {
        STATE.with(|s| s.borrow().as_ref().unwrap().events.clone())
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        STATE.with(|s| s.borrow_mut().take());
    }
}
pub(in crate::membership) fn watch(fail_at: Option<usize>) -> Guard {
    STATE.with(|s| {
        assert!(s.borrow().is_none(), "nested local-copy observation");
        *s.borrow_mut() = Some(Observation {
            fail_at,
            events: Vec::new(),
        });
    });
    Guard
}
pub(in crate::membership) fn before(
    boundary: Boundary,
    retained: usize,
    total: usize,
) -> RsfResult<()> {
    STATE.with(|s| {
        let mut state = s.borrow_mut();
        if let Some(state) = state.as_mut() {
            let index = state.events.len();
            state.events.push(Event {
                boundary,
                retained,
                total,
            });
            if state.fail_at == Some(index) {
                return Err(corrupt("injected local-copy boundary"));
            }
        }
        Ok(())
    })
}
