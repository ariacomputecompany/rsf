//! Test-only failures around actual caller operations; no power-loss claim.
use super::*;
use std::cell::RefCell;

#[derive(Clone, Debug)]
pub(in crate::learner::prefix) struct Event {
    pub operation: Operation,
    pub completed: bool,
    pub path: PathBuf,
}
type Inspection = Box<dyn FnMut(&Event)>;
struct State {
    events: Vec<Event>,
    fail_at: Option<usize>,
    inspect: Option<Inspection>,
}
thread_local! { static WATCH: RefCell<Option<State>> = const { RefCell::new(None) }; }
pub(in crate::learner::prefix) struct Watch;
impl Watch {
    pub(in crate::learner::prefix) fn events(&self) -> Vec<Event> {
        WATCH.with(|w| w.borrow().as_ref().unwrap().events.clone())
    }
}
impl Drop for Watch {
    fn drop(&mut self) {
        WATCH.with(|w| {
            w.borrow_mut().take();
        });
    }
}
pub(in crate::learner::prefix) fn watch(fail_at: Option<usize>) -> Watch {
    start(fail_at, None)
}
pub(in crate::learner::prefix) fn inspect(inspect: impl FnMut(&Event) + 'static) -> Watch {
    start(None, Some(Box::new(inspect)))
}
fn start(fail_at: Option<usize>, inspect: Option<Inspection>) -> Watch {
    WATCH.with(|w| {
        assert!(w.borrow().is_none());
        *w.borrow_mut() = Some(State {
            events: Vec::new(),
            fail_at,
            inspect,
        });
    });
    Watch
}
pub(super) fn at(operation: Operation, completed: bool, path: &Path) -> RsfResult<()> {
    WATCH.with(|w| {
        let mut w = w.borrow_mut();
        let Some(state) = w.as_mut() else {
            return Ok(());
        };
        let position = state.events.len();
        state.events.push(Event {
            operation,
            completed,
            path: path.to_path_buf(),
        });
        if let Some(inspect) = &mut state.inspect {
            inspect(state.events.last().expect("just observed actual boundary"));
        }
        if state.fail_at == Some(position) {
            Err(invalid_input(
                "injected bounded namespace boundary failure (test only)",
            ))
        } else {
            Ok(())
        }
    })
}
