//! Determine actions to synchronize the trees

use rayon::prelude::*;

use crate::diff::{DiffEntry, diff_entries};
use crate::generic::iter::CommonValueByExt as _;
use crate::generic::task_tracker::TaskTracker;
use crate::proto::{MyDirEntry, MyDirEntryExt, TimestampOrd as _};

/// Synchronization mode
#[derive(PartialEq)]
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
/// `DiffEntry` are updated when synchronization source is determined.
///
/// # Errors
/// - early exit
pub fn sync_plan(
    task_tracker: TaskTracker,
    prev_snaps: Option<Vec<MyDirEntry>>,
    mode: SyncMode,
    entries: &mut [DiffEntry],
) -> anyhow::Result<()> {
    let ctx = SyncPlanCtx {
        task_tracker,
        prev_snaps,
        mode,
    };
    ctx.sync_plan(entries)
}

struct SyncPlanCtx {
    task_tracker: TaskTracker,
    /// Previous snapshots of the trees, at the same timestamp (last synchronization)
    prev_snaps: Option<Vec<MyDirEntry>>,
    mode: SyncMode,
}
impl SyncPlanCtx {
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
        if let Some(prev_snaps) = &self.prev_snaps
            && let Some(common_prev_entry) = prev_snaps
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

        if self.mode == SyncMode::Latest {
            // Get the entry with the latest modification time
            let mut latest_entry: Option<&MyDirEntry> = None;
            let mut source_index = 0u8;
            let mut conflict = false;
            for (i, entry) in diff_entry.entries.iter().enumerate() {
                if let Some(entry) = entry {
                    if let Some(latest_entry_ref) = latest_entry {
                        match latest_entry_ref.mtime.cmp(&entry.mtime) {
                            std::cmp::Ordering::Less => {
                                // update latest_entry with latest
                                latest_entry = Some(entry);
                                source_index = i as u8;
                                conflict = false;
                            }
                            std::cmp::Ordering::Equal => {
                                // two entries with latest mtime shall be equal
                                conflict =
                                    conflict || !diff_entries(entry, latest_entry_ref).is_empty();
                            }
                            std::cmp::Ordering::Greater => {}
                        }
                    } else {
                        // first existing entry
                        latest_entry = Some(entry);
                        source_index = i as u8;
                    }
                }
            }

            if !conflict {
                // latest sync possible
                diff_entry.sync_source_index = Some(source_index);
                return Ok(());
            }
        }

        Ok(())
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
    /// - true when `new_entry` is more recent than `common_prev_entry`
    /// - false otherwise: `new_entry` is older than `common_prev_entry`
    fn mtime_is_consistent(
        new_entry: Option<&MyDirEntry>,
        common_prev_entry: Option<&MyDirEntry>,
    ) -> bool {
        match (new_entry, common_prev_entry) {
            (Some(new_entry), Some(common_prev_entry)) => {
                // allow TS equality for permission / ownership changes
                new_entry.mtime.cmp(&common_prev_entry.mtime).is_ge()
            }
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generic::task_tracker::tests::dummy_task_tracker;
    use crate::proto::{DirectoryData, MyDirEntry, RegularData, Specific};
    use prost_types::Timestamp;

    fn create_file_entry(file_name: &str, size: u64, hash: u64, mtime_sec: i64) -> MyDirEntry {
        MyDirEntry {
            file_name: file_name.to_string(),
            specific: Some(Specific::Regular(RegularData {
                size,
                hash: hash.to_be_bytes().to_vec(),
            })),
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            mtime: Some(Timestamp {
                seconds: mtime_sec,
                nanos: 0,
            }),
        }
    }

    fn create_root_with_file(file: MyDirEntry) -> MyDirEntry {
        MyDirEntry {
            file_name: "root".to_string(),
            specific: Some(Specific::Directory(DirectoryData {
                content: vec![file],
            })),
            permissions: 0o755,
            uid: 1000,
            gid: 1000,
            mtime: Some(Timestamp {
                seconds: 0,
                nanos: 0,
            }),
        }
    }

    #[test]
    fn test_sync_plan_standard_no_prev() {
        crate::generic::test::log_init();
        let task_tracker = dummy_task_tracker();
        let mut entries = vec![DiffEntry {
            rel_path: "./file.txt".to_string(),
            entries: vec![
                Some(create_file_entry("file.txt", 10, 1, 100)),
                Some(create_file_entry("file.txt", 20, 2, 200)),
            ],
            diff: crate::diff::DiffType::CONTENT,
            sync_source_index: None,
        }];

        sync_plan(task_tracker, None, SyncMode::Standard, &mut entries).unwrap();
        assert!(entries[0].sync_source_index.is_none());
    }

    #[test]
    fn test_sync_plan_latest_simple() {
        crate::generic::test::log_init();
        let task_tracker = dummy_task_tracker();
        let mut entries = vec![DiffEntry {
            rel_path: "./file.txt".to_string(),
            entries: vec![
                Some(create_file_entry("file.txt", 10, 1, 100)),
                Some(create_file_entry("file.txt", 20, 2, 200)),
            ],
            diff: crate::diff::DiffType::CONTENT,
            sync_source_index: None,
        }];

        sync_plan(task_tracker, None, SyncMode::Latest, &mut entries).unwrap();
        assert_eq!(entries[0].sync_source_index, Some(1));
    }

    #[test]
    fn test_sync_plan_latest_conflict_same_mtime_diff_content() {
        crate::generic::test::log_init();
        let task_tracker = dummy_task_tracker();
        let mut entries = vec![DiffEntry {
            rel_path: "./file.txt".to_string(),
            entries: vec![
                Some(create_file_entry("file.txt", 10, 1, 200)),
                Some(create_file_entry("file.txt", 20, 2, 200)),
            ],
            diff: crate::diff::DiffType::CONTENT,
            sync_source_index: None,
        }];

        sync_plan(task_tracker, None, SyncMode::Latest, &mut entries).unwrap();
        assert!(entries[0].sync_source_index.is_none());
    }

    #[test]
    fn test_sync_plan_standard_update_one_side() {
        crate::generic::test::log_init();
        let task_tracker = dummy_task_tracker();
        let prev = create_file_entry("file.txt", 10, 1, 100);
        let current_1 = prev.clone();
        let current_2 = create_file_entry("file.txt", 20, 2, 200);

        let mut entries = vec![DiffEntry {
            rel_path: "./file.txt".to_string(),
            entries: vec![Some(current_1), Some(current_2)],
            diff: crate::diff::DiffType::CONTENT,
            sync_source_index: None,
        }];

        let prev_root = create_root_with_file(prev);

        sync_plan(
            task_tracker,
            Some(vec![prev_root.clone(), prev_root]),
            SyncMode::Standard,
            &mut entries,
        )
        .unwrap();
        assert_eq!(entries[0].sync_source_index, Some(1));
    }

    #[test]
    fn test_sync_plan_standard_conflict() {
        crate::generic::test::log_init();
        let task_tracker = dummy_task_tracker();
        let prev = create_file_entry("file.txt", 10, 1, 100);
        let current_1 = create_file_entry("file.txt", 20, 2, 200);
        let current_2 = create_file_entry("file.txt", 30, 3, 300);

        let mut entries = vec![DiffEntry {
            rel_path: "./file.txt".to_string(),
            entries: vec![Some(current_1), Some(current_2)],
            diff: crate::diff::DiffType::CONTENT,
            sync_source_index: None,
        }];

        let prev_root = create_root_with_file(prev);

        sync_plan(
            task_tracker,
            Some(vec![prev_root.clone(), prev_root]),
            SyncMode::Standard,
            &mut entries,
        )
        .unwrap();
        assert!(entries[0].sync_source_index.is_none());
    }

    #[test]
    fn test_sync_plan_standard_deletion() {
        crate::generic::test::log_init();
        let task_tracker = dummy_task_tracker();
        let prev = create_file_entry("file.txt", 10, 1, 100);
        let current_2 = prev.clone();

        let mut entries = vec![DiffEntry {
            rel_path: "./file.txt".to_string(),
            entries: vec![None, Some(current_2)],
            diff: crate::diff::DiffType::TYPE,
            sync_source_index: None,
        }];

        let prev_root = create_root_with_file(prev);

        sync_plan(
            task_tracker,
            Some(vec![prev_root.clone(), prev_root]),
            SyncMode::Standard,
            &mut entries,
        )
        .unwrap();
        assert_eq!(entries[0].sync_source_index, Some(0));
    }

    #[test]
    fn test_sync_plan_standard_older_update_ignored() {
        crate::generic::test::log_init();
        let task_tracker = dummy_task_tracker();
        let prev = create_file_entry("file.txt", 10, 1, 200);
        let current_1 = prev.clone();
        let current_2 = create_file_entry("file.txt", 20, 2, 100);

        let mut entries = vec![DiffEntry {
            rel_path: "./file.txt".to_string(),
            entries: vec![Some(current_1), Some(current_2)],
            diff: crate::diff::DiffType::CONTENT | crate::diff::DiffType::MTIME,
            sync_source_index: None,
        }];

        let prev_root = create_root_with_file(prev);

        sync_plan(
            task_tracker,
            Some(vec![prev_root.clone(), prev_root]),
            SyncMode::Standard,
            &mut entries,
        )
        .unwrap();
        assert!(entries[0].sync_source_index.is_none());
    }

    #[test]
    fn test_sync_plan_standard_update_permission() {
        crate::generic::test::log_init();
        let task_tracker = dummy_task_tracker();
        let prev = create_file_entry("file.txt", 10, 1, 100);
        let current_1 = prev.clone();
        let current_2 = MyDirEntry {
            permissions: 0o640,
            ..prev.clone()
        };

        let mut entries = vec![DiffEntry {
            rel_path: "./file.txt".to_string(),
            entries: vec![Some(current_1), Some(current_2)],
            diff: crate::diff::DiffType::PERMISSIONS,
            sync_source_index: None,
        }];

        let prev_root = create_root_with_file(prev);

        sync_plan(
            task_tracker,
            Some(vec![prev_root.clone(), prev_root]),
            SyncMode::Standard,
            &mut entries,
        )
        .unwrap();
        assert_eq!(entries[0].sync_source_index, Some(1));
    }
}
