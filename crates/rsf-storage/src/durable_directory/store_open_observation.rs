//! Test-only return uncertainty AFTER an actual successful directory sync.
//! No failed syscall, kernel crash or power-loss simulation is claimed.
use super::*;
use std::cell::RefCell;
use std::ffi::OsString;

#[derive(Clone, Debug)]
pub(crate) struct Event {
    pub directory: PathBuf,
    pub manifest: Option<Vec<u8>>,
    pub segments: Vec<(OsString, u64)>,
}
struct State {
    root: PathBuf,
    fail_at: Option<usize>,
    events: Vec<Event>,
}
thread_local! { static STATE: RefCell<Option<State>> = const { RefCell::new(None) }; }
pub(crate) struct Watch;
impl Watch {
    pub(crate) fn events(&self) -> Vec<Event> {
        STATE.with(|state| {
            state
                .borrow()
                .as_ref()
                .expect("active watch")
                .events
                .clone()
        })
    }
}
impl Drop for Watch {
    fn drop(&mut self) {
        STATE.with(|state| {
            state.borrow_mut().take();
        });
    }
}
pub(crate) fn watch(root: &Path, fail_at: Option<usize>) -> Watch {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        assert!(state.is_none(), "publication observation cannot nest");
        *state = Some(State {
            root: root.to_path_buf(),
            fail_at,
            events: Vec::new(),
        });
    });
    Watch
}
pub(super) fn after_success(directory: &Path) -> io::Result<()> {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let Some(state) = state.as_mut() else {
            return Ok(());
        };
        let manifest = match fs::read(state.root.join("MANIFEST")) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let mut segments = Vec::new();
        for entry in fs::read_dir(state.root.join("segments"))? {
            let entry = entry?;
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "segment")
            {
                // Generic store opening permits a final segment alias. Observe
                // the actual file bytes, not the symlink's pathname length.
                segments.push((entry.file_name(), fs::metadata(entry.path())?.len()));
            }
        }
        segments.sort();
        let position = state.events.len();
        state.events.push(Event {
            directory: directory.to_path_buf(),
            manifest,
            segments,
        });
        if state.fail_at == Some(position) {
            Err(io::Error::other(
                "injected successful-sync return uncertainty (test only)",
            ))
        } else {
            Ok(())
        }
    })
}
