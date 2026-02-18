//! Execute actions to synchronize the trees

use std::collections::HashMap;
use std::sync::Arc;

use flume::Sender;
use futures::StreamExt as _;
use futures::stream::SelectAll;

use crate::diff::{self, DiffEntry};
use crate::generic::task_tracker::{TaskExit, TaskTracker};
use crate::proto::{
    ActionReq, MyDirEntry, MyDirEntryExt as _, PROTO_NULL_VALUE, Specific,
    action::{DeleteEntryReq, FileReadReq, FileWriteReq, UpdateMetadataReq},
};
use crate::tree::{ActionReqSender, ActionRspReceiver, Tree};

/// Execute actions to synchronize the trees
///
/// # Errors
/// - early exit
pub async fn sync_exec(
    task_tracker: TaskTracker,
    trees: &mut [Box<dyn Tree + Send + Sync>],
    diff_entries: &[DiffEntry],
) -> anyhow::Result<()> {
    let mut ctx = SyncExecCtx::new(task_tracker, trees, diff_entries);
    ctx.sync_exec().await
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
    tree_senders: Arc<Vec<ActionReqSender>>,
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

    async fn sync_exec(&mut self) -> anyhow::Result<()> {
        // preparation: determine actions to perform
        self.prepare();

        // spawn task to handle incoming streams from all trees
        let (end_marker_notif_sender, end_marker_notif_receiver) = flume::bounded(1);
        self.task_tracker.spawn({
            let tree_receivers = self
                .trees
                .iter()
                .map(|t| t.get_fs_action_responder())
                .collect::<Vec<_>>();
            let tree_senders = self.tree_senders.clone();
            let file_data_ctx = self.file_data_ctx.clone();
            async move {
                Self::handle_incoming_streams_task(
                    tree_receivers,
                    tree_senders,
                    end_marker_notif_sender,
                    file_data_ctx,
                )
                .await
            }
        })?;

        // 1st pass: delete entries, in reverse order to allow deletion of directories
        for diff_entry_idx in self.act_delete.iter().rev() {
            self.entry_delete(*diff_entry_idx).await?;
        }

        // 2nd pass: update entries, per source tree
        for (src_idx, act_update) in self.act_update.iter().enumerate() {
            // update all entries for source src_idx
            for ctx in act_update {
                self.entry_update(src_idx, ctx).await?;
            }

            // send end marker to source tree
            log::debug!("sync_exec[{}]: sending end_marker", src_idx + 1);
            self.tree_senders[src_idx]
                .send_async(Arc::new(ActionReq::EndMarker(PROTO_NULL_VALUE)))
                .await?;
            // wait for end marker reception
            let marker = end_marker_notif_receiver.recv_async().await?;
            debug_assert!(marker == src_idx);
            // read data have been completely processed
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
    async fn entry_delete(&self, diff_entry_idx: usize) -> anyhow::Result<()> {
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
                log::debug!("sync_exec[{}]: delete {}", dst_idx + 1, diff_entry.rel_path);
                self.tree_senders[dst_idx]
                    .send_async(Arc::new(delete_act))
                    .await?;
            }
        }
        Ok(())
    }

    /// Handle update of one entry
    async fn entry_update(&self, src_idx: usize, ctx: &FileUpdateCtx) -> anyhow::Result<()> {
        let diff_entry = &self.diff_entries[ctx.diff_entry_idx];

        if ctx.need_data_read {
            // get file content from source tree
            log::debug!(
                "sync_exec[{}]: read data from {}",
                src_idx + 1,
                diff_entry.rel_path
            );
            self.tree_senders[src_idx]
                .send_async(Arc::new(ActionReq::ReadFile(FileReadReq {
                    rel_path: diff_entry.rel_path.clone(),
                })))
                .await?;
            // received data will be forwarded to destination trees
        }

        if ctx.metadata_destination_bitmap != 0 {
            let src_entry = diff_entry.entries[src_idx].as_ref().unwrap();
            let update_act = Arc::new(ActionReq::CreateUpdateMetadata(UpdateMetadataReq {
                rel_path: diff_entry.rel_path.clone(),
                metadata: Some(src_entry.clone()),
            }));
            // update metadata on destination trees
            for dst_idx in 0..self.trees.len() {
                if (ctx.metadata_destination_bitmap & (1 << dst_idx)) != 0 {
                    log::debug!(
                        "sync_exec[{}->{}]: update metadata for {}",
                        src_idx + 1,
                        dst_idx + 1,
                        diff_entry.rel_path
                    );
                    self.tree_senders[dst_idx]
                        .send_async(update_act.clone())
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Task to handle incoming streams from all trees
    ///
    /// - forward received data from source tree to destination trees
    /// - log errors reported by trees
    /// - forward `end_marker` notifications
    async fn handle_incoming_streams_task(
        tree_receivers: Vec<ActionRspReceiver>,
        tree_senders: Arc<Vec<ActionReqSender>>,
        end_marker_notif_sender: Sender<usize>,
        file_data_ctx: Arc<HashMap<String, FileDataCtx>>,
    ) -> anyhow::Result<TaskExit> {
        let mut combined_stream = SelectAll::new();
        for (src_idx, receiver) in tree_receivers.into_iter().enumerate() {
            combined_stream.push(receiver.into_stream().map(move |event| (src_idx, event)));
        }

        while let Some((src_idx, event)) = combined_stream.next().await {
            match event {
                crate::proto::ActionRsp::EndMarker(_) => {
                    log::debug!("sync_exec[{}]: end_marker received", src_idx + 1);
                    end_marker_notif_sender.send_async(src_idx).await?;
                }
                crate::proto::ActionRsp::Error(error_rsp) => {
                    log::warn!("sync_exec[{}]: {error_rsp:?}", src_idx + 1);
                }
                crate::proto::ActionRsp::FileData(file_read_rsp) => {
                    // prepare data write request
                    let file_ctx = file_data_ctx.get(&file_read_rsp.rel_path).unwrap();
                    let file_size = {
                        let Some(Specific::Regular(reg_data)) = &file_ctx.entry.specific else {
                            unreachable!()
                        };
                        reg_data.size
                    };

                    let current_segment =
                        1 + file_read_rsp.offset / file_read_rsp.data.len() as u64;
                    let nb_segments = file_size.div_ceil(file_read_rsp.data.len() as u64);

                    let size = (file_read_rsp.offset == 0).then_some(file_size);
                    let metadata = (file_read_rsp.offset + file_read_rsp.data.len() as u64
                        == file_size)
                        .then(|| file_ctx.entry.clone());
                    let file_write_req = Arc::new(ActionReq::CreateUpdateFile(FileWriteReq {
                        rel_path: file_read_rsp.rel_path,
                        offset: file_read_rsp.offset,
                        data: file_read_rsp.data,
                        size,
                        metadata,
                    }));
                    let rel_path = {
                        let ActionReq::CreateUpdateFile(FileWriteReq { rel_path, .. }) =
                            &*file_write_req
                        else {
                            unreachable!();
                        };
                        rel_path
                    };

                    // forward received data to destination trees
                    for dst_idx in 0..tree_senders.len() {
                        if (file_ctx.destination_bitmap & (1 << dst_idx)) != 0 {
                            log::debug!(
                                "sync_exec[{}->{}]: forward data segment {current_segment}/{nb_segments} for {rel_path}",
                                src_idx + 1,
                                dst_idx + 1,
                            );
                            tree_senders[dst_idx]
                                .send_async(file_write_req.clone())
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(TaskExit::SecondaryTaskKeepRunning)
    }
}
