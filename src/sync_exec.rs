//! Execute actions to synchronize the trees

use std::collections::HashMap;
use std::sync::Arc;

use crate::diff::{self, DiffEntry};
use crate::generic::task_tracker::{TaskExit, TaskTracker};
use crate::proto::{
    ActionReq, MyDirEntry, MyDirEntryExt as _, Specific,
    action::{DeleteEntryReq, FileReadReq, UpdateMetadataReq},
};
use crate::tree::Tree;

/// Execute actions to synchronize the trees
///
/// # Errors
/// - early exit
pub fn sync_exec(
    task_tracker: TaskTracker,
    trees: &mut [Box<dyn Tree + Send + Sync>],
    diff_entries: &[DiffEntry],
) -> anyhow::Result<()> {
    let mut ctx = SyncExecCtx::new(task_tracker, trees, diff_entries);
    ctx.sync_exec()
}

/// Context to update one regular file content
struct FileDataCtx {
    /// entry to update
    entry: MyDirEntry,
    /// bitmap of destination trees
    destination_bitmap: u32,
}

/// Context to update one file
struct FileUpdateCtx {
    diff_entry_idx: usize,
    /// bitmap of destination trees for metadata only
    metadata_destination_bitmap: u32,
    /// whether file data needs to be read from source
    need_data_read: bool,
}

struct SyncExecCtx<'a> {
    task_tracker: TaskTracker,
    /// synchronization trees
    trees: &'a mut [Box<dyn Tree + Send + Sync>],
    /// list of diff/sync entries
    diff_entries: &'a [DiffEntry],
    /// Send actions to each tree
    tree_senders: Arc<Vec<flume::Sender<ActionReq>>>,
    /// Indexes of `diff_entries` to delete
    act_delete: Vec<usize>,
    /// Files to update (metadata and/or content), per source tree
    act_update: Vec<Vec<FileUpdateCtx>>,
    /// Context for all regular files to be updated, key is file's relative path
    file_data_ctx: Arc<HashMap<String, FileDataCtx>>,
}
impl<'a> SyncExecCtx<'a> {
    fn new(
        task_tracker: TaskTracker,
        trees: &'a mut [Box<dyn Tree + Send + Sync>],
        diff_entries: &'a [DiffEntry],
    ) -> Self {
        let tree_senders = Arc::new(
            trees
                .iter()
                .map(|t| t.get_fs_action_requester())
                .collect::<Vec<_>>(),
        );
        let act_update = std::iter::repeat_with(|| Vec::with_capacity(diff_entries.len()))
            .take(trees.len())
            .collect();
        SyncExecCtx {
            task_tracker,
            trees,
            diff_entries,
            tree_senders,
            act_delete: Vec::with_capacity(diff_entries.len()),
            act_update,
            file_data_ctx: Arc::new(HashMap::new()),
        }
    }

    fn sync_exec(&mut self) -> anyhow::Result<()> {
        // preparation: determine actions to perform
        self.prepare();

        // 1st pass: delete entries, in reverse order to allow deletion of directories
        for diff_entry_idx in self.act_delete.iter().rev() {
            self.task_tracker.get_shutdown_req_as_result()?;
            self.entry_delete(*diff_entry_idx)?;
        }

        Ok(())
    }

    /// Go through `self.diff_entries` and fill `self.act_delete` & `self.act_update` & `self.file_data_ctx`
    fn prepare(&mut self) {
        let mut file_data_ctx = HashMap::with_capacity(self.diff_entries.len());

        for (i, diff_entry) in self.diff_entries.iter().enumerate() {
            if let Some(src_idx) = diff_entry.sync_source_index {
                let src_idx = src_idx as usize;
                if let Some(src_entry) = &diff_entry.entries[src_idx] {
                    // source has entry => update entry
                    // determine how to update
                    let mut metadata_destination_bitmap = 0;
                    let mut content_destination_bitmap = 0;
                    if let Some(Specific::Regular(reg_data_src)) = &src_entry.specific
                        && reg_data_src.size != 0
                    {
                        // regular file with content
                        for dst_idx in 0..self.trees.len() {
                            if dst_idx != src_idx {
                                if let Some(dst_entry) = &diff_entry.entries[dst_idx] {
                                    let diff = diff::diff_entries(src_entry, dst_entry);
                                    if diff.contains(diff::DiffType::CONTENT) {
                                        // file content to be update
                                        content_destination_bitmap |= 1 << dst_idx;
                                    } else if !diff.is_empty() {
                                        // file metadata to be updated
                                        metadata_destination_bitmap |= 1 << dst_idx;
                                    }
                                    // else: no diff, nothing to update
                                } else {
                                    // no file in destination tree => content to be updated
                                    content_destination_bitmap |= 1 << dst_idx;
                                }
                            }
                        }
                    } else {
                        // empty file or other file type => metadata only
                        for dst_idx in 0..self.trees.len() {
                            if dst_idx != src_idx {
                                if let Some(dst_entry) = &diff_entry.entries[dst_idx] {
                                    let diff = diff::diff_entries(src_entry, dst_entry);
                                    if !diff.is_empty() {
                                        // file metadata to be updated
                                        metadata_destination_bitmap |= 1 << dst_idx;
                                    }
                                    // else: no diff, nothing to update
                                } else {
                                    // no file in destination tree => entry to be created
                                    metadata_destination_bitmap |= 1 << dst_idx;
                                }
                            }
                        }
                    }
                    if content_destination_bitmap != 0 {
                        file_data_ctx.insert(
                            diff_entry.rel_path.clone(),
                            FileDataCtx {
                                entry: src_entry.clone(),
                                destination_bitmap: content_destination_bitmap,
                            },
                        );
                    }
                    self.act_update[src_idx].push(FileUpdateCtx {
                        diff_entry_idx: i,
                        metadata_destination_bitmap,
                        need_data_read: content_destination_bitmap != 0,
                    });
                } else {
                    // source has no entry => delete entry
                    self.act_delete.push(i);
                }
            }
        }
        self.file_data_ctx = Arc::new(file_data_ctx);
    }

    /// Handle deletion of one entry
    fn entry_delete(&self, diff_entry_idx: usize) -> anyhow::Result<()> {
        let diff_entry = &self.diff_entries[diff_entry_idx];
        for dst_idx in 0..self.trees.len() {
            if let Some(entry) = &diff_entry.entries[dst_idx] {
                // entry exist for this destination tree => delete it
                let delete_act = if entry.is_dir() {
                    ActionReq::DeleteDir(DeleteEntryReq {
                        rel_path: diff_entry.rel_path.clone(),
                    })
                } else {
                    ActionReq::DeleteFile(DeleteEntryReq {
                        rel_path: diff_entry.rel_path.clone(),
                    })
                };
                self.tree_senders[dst_idx].send(delete_act)?;
            }
        }
        Ok(())
    }

    /// Handle update of one entry
    fn entry_update(&self, src_idx: usize, ctx: &FileUpdateCtx) -> anyhow::Result<()> {
        let diff_entry = &self.diff_entries[ctx.diff_entry_idx];

        if ctx.need_data_read {
            // get file content from source tree
            self.tree_senders[src_idx].send(ActionReq::ReadFile(FileReadReq {
                rel_path: diff_entry.rel_path.clone(),
            }))?;
            // received data will be forwarded to destination trees
        }

        if ctx.metadata_destination_bitmap != 0 {
            let src_entry = diff_entry.entries[src_idx].as_ref().unwrap();
            // update metadata on destination trees
            for dst_idx in 0..self.trees.len() {
                if (ctx.metadata_destination_bitmap & (1 << dst_idx)) != 0 {
                    self.tree_senders[dst_idx].send(ActionReq::CreateUpdateMetadata(
                        UpdateMetadataReq {
                            rel_path: diff_entry.rel_path.clone(),
                            metadata: Some(src_entry.clone()),
                        },
                    ))?;
                }
            }
        }

        Ok(())
    }

    /// Task to forward received data from source tree to destination trees
    async fn data_forward_task() -> anyhow::Result<TaskExit> {
        todo!();
    }
}
