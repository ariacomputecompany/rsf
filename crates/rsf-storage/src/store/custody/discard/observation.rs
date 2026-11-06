//! Test-only boundary failures around actual unlink/rmdir/fsync operations.
//! Injected errors are not machine power cuts or proof of disk persistence.
use super::*;
use std::cell::RefCell;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Event {
    pub operation: remove::Operation,
    pub completed: bool,
    pub path: PathBuf,
}
struct State {
    events: Vec<Event>,
    fail_at: Option<usize>,
}
thread_local! {static WATCH:RefCell<Option<State>>=const{RefCell::new(None)};}
pub(super) struct Watch;
impl Watch {
    pub(super) fn events(&self) -> Vec<Event> {
        WATCH.with(|v| v.borrow().as_ref().unwrap().events.clone())
    }
}
impl Drop for Watch {
    fn drop(&mut self) {
        WATCH.with(|v| {
            v.borrow_mut().take();
        });
    }
}
pub(super) fn watch(fail_at: Option<usize>) -> Watch {
    WATCH.with(|v| {
        assert!(v.borrow().is_none());
        *v.borrow_mut() = Some(State {
            events: Vec::new(),
            fail_at,
        });
    });
    Watch
}
pub(super) fn at(operation: remove::Operation, completed: bool, path: &Path) -> RsfResult<()> {
    WATCH.with(|v| {
        let mut slot = v.borrow_mut();
        let Some(state) = slot.as_mut() else {
            return Ok(());
        };
        let position = state.events.len();
        state.events.push(Event {
            operation,
            completed,
            path: path.to_path_buf(),
        });
        if state.fail_at == Some(position) {
            Err(custody_error(
                "injected discard boundary failure (test only)",
            ))
        } else {
            Ok(())
        }
    })
}

// A producer-side test-only partial-write/interruption seam. Bytes and actual
// descriptor come from the real owned rotation helper, never a second codec.
thread_local! {static ROTATION:RefCell<Option<usize>>=const{RefCell::new(None)};}
pub(super) struct Rotation;
impl Drop for Rotation {
    fn drop(&mut self) {
        ROTATION.with(|v| {
            v.borrow_mut().take();
        });
    }
}
pub(super) fn stop_rotation_after_prefix(bytes: usize) -> Rotation {
    ROTATION.with(|v| {
        assert!(v.borrow().is_none());
        *v.borrow_mut() = Some(bytes);
    });
    Rotation
}
pub(in crate::store::custody) fn rotation_after_open(
    file: &mut File,
    _path: &Path,
    canonical: &[u8],
    previous: usize,
) -> RsfResult<()> {
    let prefix = ROTATION.with(|v| v.borrow_mut().take());
    if let Some(prefix) = prefix {
        assert_eq!(
            previous, 0,
            "fixture interrupts fresh actual manifest creation"
        );
        file.write_all(&canonical[..prefix.min(canonical.len())])
            .map_err(custody_io)?;
        file.sync_all().map_err(custody_io)?;
        return Err(custody_error(
            "injected actual rotation temporary interruption (test only)",
        ));
    }
    Ok(())
}
