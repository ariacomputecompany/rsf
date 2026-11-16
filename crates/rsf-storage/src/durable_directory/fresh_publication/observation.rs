//! Same-thread physical fault schedules for public fresh opens, never authority.
use std::cell::RefCell;
use std::io;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    RoutesPublished,
    SegmentBound,
    BeforeFileOpen,
    FinalOpened,
    FinalSynced,
}
type Action = Box<dyn FnMut(Phase) -> io::Result<()>>;
thread_local! { static ACTION: RefCell<Option<Action>> = RefCell::new(None); }
pub(crate) struct Watch;
impl Drop for Watch {
    fn drop(&mut self) {
        ACTION.with(|action| {
            action.borrow_mut().take();
        });
    }
}
pub(crate) fn watch(action: impl FnMut(Phase) -> io::Result<()> + 'static) -> Watch {
    ACTION.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(action));
    });
    Watch
}
pub(crate) fn at(phase: Phase) -> io::Result<()> {
    ACTION.with(|slot| match slot.borrow_mut().as_mut() {
        Some(action) => action(phase),
        None => Ok(()),
    })
}
