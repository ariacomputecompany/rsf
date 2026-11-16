//! Retain derived coordination while the peer retains all authoritative cells.
use super::*;
use rsf_vsr::VsrStateToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatorState {
    Recovered,
    Retained,
}

type Tokens = BTreeMap<PlacementGroupId, VsrStateToken>;
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CoordinatorCacheStats {
    pub full_recoveries: u64,
    pub reuses: u64,
}
#[derive(Debug)]
struct Entry<T: VsrTransport> {
    router: VsrBackedFabricRouter<T>,
    tokens: Tokens,
    storage: SegmentStoreRevision,
}
#[derive(Debug)]
pub struct InstalledCoordinatorCache<T: VsrTransport> {
    entry: Option<Entry<T>>,
    stats: CoordinatorCacheStats,
}
impl<T: VsrTransport> Default for InstalledCoordinatorCache<T> {
    fn default() -> Self {
        Self {
            entry: None,
            stats: CoordinatorCacheStats::default(),
        }
    }
}
impl<T: VsrTransport> InstalledCoordinatorCache<T> {
    pub fn stats(&self) -> CoordinatorCacheStats {
        self.stats
    }
    pub fn invalidate(&mut self) {
        self.entry = None;
    }

    /// Reuse only exact live cells and unchanged derived protocol storage. The
    /// callback must still reconcile application state and confirm fresh quorum.
    /// Any failure/unwind drops derived state and restores exclusive cell custody.
    pub fn with_cells<R>(
        &mut self,
        path: impl AsRef<Path>,
        coordination_epoch: Epoch,
        local: NodeId,
        cells: &mut BTreeMap<PlacementGroupId, DurableVsrCell>,
        transport: T,
        operation: impl FnOnce(&mut VsrBackedFabricRouter<T>, CoordinatorState) -> RsfResult<R>,
    ) -> RsfResult<R> {
        let previous = self.entry.take();
        let current = tokens(cells)?;
        let ready = match previous {
            Some(mut entry)
                if entry.tokens == current
                    && entry.router.path == path.as_ref()
                    && entry.router.epoch == coordination_epoch
                    && entry.router.primary == local =>
            {
                if entry.router.router.storage_revision()? == entry.storage {
                    Some(entry.router)
                } else {
                    None
                }
            }
            _ => None,
        };
        let state = if ready.is_some() {
            CoordinatorState::Retained
        } else {
            CoordinatorState::Recovered
        };
        let router = if let Some(mut router) = ready {
            self.stats.reuses = self.stats.reuses.saturating_add(1);
            router.cells = std::mem::take(cells);
            router.transport = transport;
            router
        } else {
            self.stats.full_recoveries = self.stats.full_recoveries.saturating_add(1);
            VsrBackedFabricRouter::recover_installed_cells(
                path.as_ref(),
                coordination_epoch,
                local,
                cells,
                transport,
            )?
        };
        let mut custody = CachedCustody {
            router: Some(router),
            owner: cells,
            destination: &mut self.entry,
            next: None,
        };
        let router = custody.router.as_mut().expect("owned coordinator");
        let before = router.router.storage_revision()?;
        let result = operation(router, state)?;
        let storage = router.router.storage_revision()?;
        if !storage.same_writer_epoch(&before) {
            return Err(handoff_corrupt(
                "protocol storage changed through another writer during coordination; recover before retry",
            ));
        }
        let next = (tokens(&router.cells)?, storage);
        custody.next = Some(next);
        Ok(result)
    }
}
fn tokens(cells: &BTreeMap<PlacementGroupId, DurableVsrCell>) -> RsfResult<Tokens> {
    cells
        .iter()
        .map(|(pg, cell)| Ok((*pg, cell.state_token()?)))
        .collect()
}
struct CachedCustody<'a, T: VsrTransport> {
    router: Option<VsrBackedFabricRouter<T>>,
    owner: &'a mut BTreeMap<PlacementGroupId, DurableVsrCell>,
    destination: &'a mut Option<Entry<T>>,
    next: Option<(Tokens, SegmentStoreRevision)>,
}
impl<T: VsrTransport> Drop for CachedCustody<'_, T> {
    fn drop(&mut self) {
        let mut router = self.router.take().expect("owned coordinator");
        *self.owner = std::mem::take(&mut router.cells);
        if let Some((tokens, storage)) = self.next.take() {
            *self.destination = Some(Entry {
                router,
                tokens,
                storage,
            });
        }
    }
}
