//! Determine actions to synchronize the trees

use rayon::prelude::*;

use crate::diff::{DiffEntry, diff_entries};
use crate::generic::iter::CommonValueByExt as _;
use crate::generic::task_tracker::TaskTracker;
use crate::proto::{MyDirEntry, MyDirEntryExt, TimestampExt as _};

/// Synchronization mode
pub enum SyncMode {
    /// Standard mode: use previous sync state to determine how to resolve changes
    /// If the ancestor is not common, no action is proposed
    Standard,
    /// Use standard mode if possible; otherwise, propose to take the latest file
    /// (latest modification time)
    Latest,
}

/// Plan actions to synchronize the trees
///
/// When successful, the returned vector has the same length as the input `diff_entries`,
/// and each `SyncEntry` matches the associated `DiffEntry`.
///
/// # Errors
/// - early exit
pub fn sync_plan(
    task_tracker: TaskTracker,
    prev_snaps: Vec<MyDirEntry>, // may be empty if there is no snapshot with same ts available for all trees
    mode: SyncMode,
    entries: &mut [DiffEntry],
) -> anyhow::Result<()> {
    let ctx = SyncCtx {
        task_tracker,
        prev_snaps,
        mode,
    };
    ctx.sync_plan(entries)
}

struct SyncCtx {
    task_tracker: TaskTracker,
    /// Previous snapshots of the trees, at the same timestamp (last synchronization)
    prev_snaps: Vec<MyDirEntry>,
    mode: SyncMode,
}
impl SyncCtx {
    fn sync_plan(&self, entries: &mut [DiffEntry]) -> anyhow::Result<()> {
        log::info!("sync_plan: starting");
        // parallelized: process each diff entry and generate the matching sync entry
        entries
            .par_iter_mut()
            .try_for_each(|diff_entry| self.sync_entry(diff_entry))?;
        log::info!("sync_plan: completed");
        Ok(())
    }

    /// Determine the action to perform the synchronization for one entry
    #[allow(clippy::cast_possible_truncation)]
    fn sync_entry(&self, diff_entry: &mut DiffEntry) -> anyhow::Result<()> {
        self.task_tracker.get_shutdown_req_as_result()?;

        // standard sync: need a common ancestor
        if !self.prev_snaps.is_empty()
            && let Some(common_prev_entry) = self
                .prev_snaps
                .iter()
                .map(|e| e.get_entry(&diff_entry.rel_path))
                .common_value_by(|a, b| Self::are_same_entries(*a, *b))
        {
            // files was the same on all trees
            // check & determine the updated file
            let mut new_entry_opt = None;
            let mut conflict = false;
            let mut source_index = 0u8;
            for (i, entry) in diff_entry.entries.iter().enumerate() {
                if Self::are_same_entries(entry.as_ref(), common_prev_entry) {
                    // not modified, to be updated
                } else if let Some(new_entry) = new_entry_opt {
                    if Self::are_same_entries(entry.as_ref(), new_entry) {
                        // same modification on different trees: fine, already up to date
                    } else {
                        // conflicting changes on different trees
                        conflict = true;
                        break;
                    }
                } else {
                    // first update found
                    new_entry_opt = Some(entry.as_ref());
                    source_index = i as u8;
                }
            }

            let new_entry = new_entry_opt.unwrap();
            if !conflict && Self::mtime_is_consistent(new_entry, common_prev_entry) {
                // standard sync possible
                diff_entry.sync_source_index = Some(source_index);
                return Ok(());
            }
        }

        todo!();
    }

    /// Compare 2 entries to determine the common ancestor if any
    fn are_same_entries(a: Option<&MyDirEntry>, b: Option<&MyDirEntry>) -> bool {
        match (a, b) {
            (Some(a), Some(b)) => diff_entries(a, b).is_empty(),
            (None, None) => true,
            _ => false,
        }
    }

    /// Check mtime consistency
    ///
    /// # Returns:
    /// - true when `new_entry` is None (deleted file)
    /// - true when `common_prev_entry` is None (new file)
    /// - true when `new_entry` is more recent tha`common_prev_entry`
    /// - false otherwise: `new_entry` is older tha`common_prev_entry`
    fn mtime_is_consistent(
        new_entry: Option<&MyDirEntry>,
        common_prev_entry: Option<&MyDirEntry>,
    ) -> bool {
        match (new_entry, common_prev_entry) {
            (Some(new_entry), Some(common_prev_entry)) => new_entry
                .mtime
                .unwrap()
                .cmp(&common_prev_entry.mtime.unwrap())
                .is_gt(),
            _ => true,
        }
    }
}
