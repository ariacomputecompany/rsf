//! Thread-local, test-only physical fault boundaries; normal I/O is unchanged.
use std::{
    cell::{Cell, RefCell},
    path::Path,
    rc::Rc,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::learner) enum Boundary {
    BeforeAppend,
    AfterClosedRenewal,
    AfterActiveRenewal,
    BeforeOrdinaryDuplicate,
    BeforeHandoff,
}
type Fault = Box<dyn FnOnce(&Path)>;
type PendingFault = (Boundary, Fault, Rc<Cell<usize>>);
thread_local! { static PENDING: RefCell<Option<PendingFault>> = const { RefCell::new(None) }; }
pub(in crate::learner) struct Watch(Rc<Cell<usize>>);
impl Watch {
    pub(in crate::learner) fn calls(&self) -> usize {
        self.0.get()
    }
}
impl Drop for Watch {
    fn drop(&mut self) {
        PENDING.with(|pending| {
            pending.borrow_mut().take();
        });
    }
}
pub(in crate::learner) fn fault(boundary: Boundary, fault: impl FnOnce(&Path) + 'static) -> Watch {
    let calls = Rc::new(Cell::new(0));
    PENDING.with(|pending| {
        assert!(pending.borrow().is_none());
        *pending.borrow_mut() = Some((boundary, Box::new(fault), calls.clone()));
    });
    Watch(calls)
}
pub(in crate::learner) fn at(boundary: Boundary, path: &Path) {
    let fault = PENDING.with(|pending| {
        let mut pending = pending.borrow_mut();
        if pending.as_ref().is_some_and(|(at, _, _)| *at == boundary) {
            pending.take()
        } else {
            None
        }
    });
    if let Some((_, fault, calls)) = fault {
        calls.set(calls.get() + 1);
        fault(path);
    }
}
