//! Private deterministic initial disposal interruption boundaries, test-only.
use super::*;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Boundary {
    StorageChildren,
    Cell,
    Descriptor,
}
type Fault = Box<dyn FnOnce(&Path) -> RsfResult<()>>;
type PendingFault = (Boundary, Fault, Rc<Cell<usize>>);
thread_local! { static PENDING: RefCell<Option<PendingFault>> = const { RefCell::new(None) }; }
pub(super) struct Watch(Rc<Cell<usize>>);
impl Watch {
    pub(super) fn calls(&self) -> usize {
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
pub(super) fn fault(at: Boundary, fault: impl FnOnce(&Path) -> RsfResult<()> + 'static) -> Watch {
    let count = Rc::new(Cell::new(0));
    PENDING.with(|pending| {
        assert!(pending.borrow().is_none());
        *pending.borrow_mut() = Some((at, Box::new(fault), count.clone()));
    });
    Watch(count)
}
pub(super) fn at(at: Boundary, path: &Path) -> RsfResult<()> {
    let fault = PENDING.with(|pending| {
        let mut value = pending.borrow_mut();
        if value
            .as_ref()
            .is_some_and(|(boundary, _, _)| *boundary == at)
        {
            value.take()
        } else {
            None
        }
    });
    if let Some((_, fault, count)) = fault {
        count.set(count.get() + 1);
        fault(path)?;
    }
    Ok(())
}
