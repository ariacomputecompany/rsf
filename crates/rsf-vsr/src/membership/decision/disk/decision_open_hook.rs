use std::{cell::RefCell, path::Path};
type Callback = Box<dyn FnOnce(&Path)>;
thread_local! { static HOOK: RefCell<Option<Callback>> = const { RefCell::new(None) }; }
pub(in crate::membership) fn after_open(root: &Path) {
    let callback = HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(callback) = callback {
        callback(root);
    }
}
pub(in crate::membership) struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
pub(in crate::membership) fn install(callback: impl FnOnce(&Path) + 'static) -> Guard {
    HOOK.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(callback));
    });
    Guard
}
