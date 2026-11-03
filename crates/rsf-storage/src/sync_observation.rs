//! Test-only observation of the real append sync boundary. Injected errors are
//! simulated failures, not physical power-loss or guaranteed data-loss models.
use super::*;
use std::cell::RefCell;

#[derive(Clone, Debug)]
pub(crate) struct Event {
    pub path: PathBuf,
    pub policy: SyncPolicy,
    pub manifest: Vec<u8>,
    pub completed: bool,
}
struct Observation {
    root: PathBuf,
    fail_at: Option<usize>,
    events: Vec<Event>,
}
thread_local! {
    static OBSERVATION: RefCell<Option<Observation>> = const { RefCell::new(None) };
}
pub(crate) struct Guard;
impl Guard {
    pub fn events(&self) -> Vec<Event> {
        OBSERVATION.with(|slot| slot.borrow().as_ref().unwrap().events.clone())
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        OBSERVATION.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
pub(crate) fn watch(root: &Path, fail_at: Option<usize>) -> Guard {
    OBSERVATION.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(slot.is_none(), "nested sync observations");
        *slot = Some(Observation {
            root: root.to_path_buf(),
            fail_at,
            events: Vec::new(),
        });
    });
    Guard
}
pub(crate) fn before(path: &Path, policy: SyncPolicy) -> RsfResult<Option<usize>> {
    OBSERVATION.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(state) = slot.as_mut().filter(|state| path.starts_with(&state.root)) else {
            return Ok(None);
        };
        let position = state.events.len();
        state.events.push(Event {
            path: path.to_path_buf(),
            policy,
            manifest: std::fs::read(state.root.join("MANIFEST")).unwrap(),
            completed: false,
        });
        if state.fail_at == Some(position) {
            return Err(storage_error(
                StorageIoOperation::Sync,
                io::Error::other("injected append sync failure (test only)"),
            ));
        }
        Ok(Some(position))
    })
}
pub(crate) fn after(position: Option<usize>, completed: bool) {
    if let Some(position) = position {
        OBSERVATION.with(|slot| {
            slot.borrow_mut().as_mut().unwrap().events[position].completed = completed;
        });
    }
}
