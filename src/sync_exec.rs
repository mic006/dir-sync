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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generic::task_tracker::TaskTrackerMain;
    use crate::proto::RegularData;
    use prost_types::Timestamp;

    struct MockTree {
        // Channel to receive requests sent by sync_exec (for verification in tests)
        test_req_rx: flume::Receiver<Arc<ActionReq>>,
        // Channel to give to sync_exec (via get_fs_action_requester)
        tree_req_tx: flume::Sender<Arc<ActionReq>>,

        // Channel to send responses to sync_exec (simulating tree response)
        test_rsp_tx: flume::Sender<crate::proto::ActionRsp>,
        // Channel to give to sync_exec (via get_fs_action_responder)
        tree_rsp_rx: flume::Receiver<crate::proto::ActionRsp>,
    }

    impl MockTree {
        fn new() -> Self {
            let (tree_req_tx, test_req_rx) = flume::unbounded();
            let (test_rsp_tx, tree_rsp_rx) = flume::unbounded();
            Self {
                test_req_rx,
                tree_req_tx,
                test_rsp_tx,
                tree_rsp_rx,
            }
        }
    }

    impl crate::tree::TreeMetadata for MockTree {
        fn get_entry(&self, _rel_path: &str) -> Option<&MyDirEntry> {
            None
        }

        fn get_dir_content(&self, _rel_path: &str) -> &[MyDirEntry] {
            &[]
        }
    }

    #[async_trait::async_trait]
    impl Tree for MockTree {
        async fn wait_for_tree(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        fn get_fs_action_requester(&self) -> ActionReqSender {
            self.tree_req_tx.clone()
        }

        fn get_fs_action_responder(&self) -> ActionRspReceiver {
            self.tree_rsp_rx.clone()
        }

        fn save_snap(&mut self, _sync: bool) {}

        fn take_prev_sync_snap(&mut self) -> Option<crate::proto::MetadataSnap> {
            None
        }
    }

    fn create_file_entry(name: &str, size: u64, hash: u8) -> MyDirEntry {
        MyDirEntry {
            file_name: name.to_string(),
            specific: Some(Specific::Regular(RegularData {
                size,
                hash: vec![hash],
            })),
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            mtime: Some(Timestamp {
                seconds: 1000,
                nanos: 0,
            }),
        }
    }

    #[tokio::test]
    async fn test_sync_exec_delete() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let mut task_tracker_main = TaskTrackerMain::default();
        let task_tracker = task_tracker_main.tracker();

        let tree1 = MockTree::new();
        let tree2 = MockTree::new();

        let t1_req_rx = tree1.test_req_rx.clone();
        let t1_rsp_tx = tree1.test_rsp_tx.clone();
        let t2_req_rx = tree2.test_req_rx.clone();
        let t2_rsp_tx = tree2.test_rsp_tx.clone();

        let mut trees: Vec<Box<dyn Tree + Send + Sync>> = vec![Box::new(tree1), Box::new(tree2)];

        // Entry exists in tree2 but not tree1 (source is tree1, so delete in tree2)
        let diff_entry = DiffEntry {
            rel_path: "file.txt".to_string(),
            entries: vec![None, Some(create_file_entry("file.txt", 10, 1))],
            diff: crate::diff::DiffType::TYPE,
            sync_source_index: Some(0),
        };

        let diff_entries = vec![diff_entry];

        // Spawn sync_exec
        let handle =
            tokio::spawn(async move { sync_exec(task_tracker, &mut trees, &diff_entries).await });

        // Expect delete on tree2
        let req = t2_req_rx.recv_async().await?;
        if let ActionReq::DeleteFile(del_req) = &*req {
            assert_eq!(del_req.rel_path, "file.txt");
        } else {
            panic!("Expected DeleteFile, got {req:?}");
        }

        // Expect end marker on tree1 (source)
        let req = t1_req_rx.recv_async().await?;
        if let ActionReq::EndMarker(_) = &*req {
            // Send back end marker response
            t1_rsp_tx
                .send_async(crate::proto::ActionRsp::EndMarker(PROTO_NULL_VALUE))
                .await?;
        } else {
            panic!("Expected EndMarker on tree1, got {req:?}");
        }

        // Expect end marker on tree2
        let req = t2_req_rx.recv_async().await?;
        if let ActionReq::EndMarker(_) = &*req {
            // Send back end marker response
            t2_rsp_tx
                .send_async(crate::proto::ActionRsp::EndMarker(PROTO_NULL_VALUE))
                .await?;
        } else {
            panic!("Expected EndMarker on tree2, got {req:?}");
        }

        handle.await??;

        task_tracker_main.request_stop();
        task_tracker_main.wait().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_sync_exec_update_content() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let mut task_tracker_main = TaskTrackerMain::default();
        let task_tracker = task_tracker_main.tracker();

        let tree1 = MockTree::new();
        let tree2 = MockTree::new();

        let t1_req_rx = tree1.test_req_rx.clone();
        let t1_rsp_tx = tree1.test_rsp_tx.clone();
        let t2_req_rx = tree2.test_req_rx.clone();
        let t2_rsp_tx = tree2.test_rsp_tx.clone();

        let mut trees: Vec<Box<dyn Tree + Send + Sync>> = vec![Box::new(tree1), Box::new(tree2)];

        // Update content from tree1 to tree2
        let diff_entry = DiffEntry {
            rel_path: "file.txt".to_string(),
            entries: vec![
                Some(create_file_entry("file.txt", 5, 1)),
                Some(create_file_entry("file.txt", 5, 2)), // same size but content diff (hash)
            ],
            diff: crate::diff::DiffType::CONTENT,
            sync_source_index: Some(0),
        };

        let diff_entries = vec![diff_entry];

        let handle =
            tokio::spawn(async move { sync_exec(task_tracker, &mut trees, &diff_entries).await });

        // Expect ReadFile on tree1
        let req = t1_req_rx.recv_async().await?;
        if let ActionReq::ReadFile(read_req) = &*req {
            assert_eq!(read_req.rel_path, "file.txt");
        } else {
            panic!("Expected ReadFile on tree1, got {req:?}");
        }

        // Send FileData back from tree1
        t1_rsp_tx
            .send_async(crate::proto::ActionRsp::FileData(
                crate::proto::action::FileReadRsp {
                    rel_path: "file.txt".to_string(),
                    offset: 0,
                    data: b"hello".to_vec(),
                },
            ))
            .await?;

        // Expect CreateUpdateFile on tree2
        let req = t2_req_rx.recv_async().await?;
        if let ActionReq::CreateUpdateFile(write_req) = &*req {
            assert_eq!(write_req.rel_path, "file.txt");
            assert_eq!(write_req.offset, 0);
            assert_eq!(write_req.data, b"hello");
            assert!(write_req.metadata.is_some()); // Last segment (and only segment)
        } else {
            panic!("Expected CreateUpdateFile on tree2, got {req:?}");
        }

        // Expect EndMarker on tree1
        let req = t1_req_rx.recv_async().await?;
        if let ActionReq::EndMarker(_) = &*req {
            t1_rsp_tx
                .send_async(crate::proto::ActionRsp::EndMarker(PROTO_NULL_VALUE))
                .await?;
        } else {
            panic!("Expected EndMarker on tree1, got {req:?}");
        }

        // Expect EndMarker on tree2
        let req = t2_req_rx.recv_async().await?;
        if let ActionReq::EndMarker(_) = &*req {
            t2_rsp_tx
                .send_async(crate::proto::ActionRsp::EndMarker(PROTO_NULL_VALUE))
                .await?;
        } else {
            panic!("Expected EndMarker on tree2, got {req:?}");
        }

        handle.await??;

        task_tracker_main.request_stop();
        task_tracker_main.wait().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_sync_exec_update_metadata_dir_symlink() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let mut task_tracker_main = TaskTrackerMain::default();
        let task_tracker = task_tracker_main.tracker();

        let tree1 = MockTree::new();
        let tree2 = MockTree::new();

        let t1_req_rx = tree1.test_req_rx.clone();
        let t1_rsp_tx = tree1.test_rsp_tx.clone();
        let t2_req_rx = tree2.test_req_rx.clone();
        let t2_rsp_tx = tree2.test_rsp_tx.clone();

        let mut trees: Vec<Box<dyn Tree + Send + Sync>> = vec![Box::new(tree1), Box::new(tree2)];

        // 1. Directory with different permissions
        let dir_src = MyDirEntry {
            file_name: "dir".to_string(),
            specific: Some(Specific::Directory(crate::proto::DirectoryData {
                content: vec![],
            })),
            permissions: 0o755,
            uid: 1000,
            gid: 1000,
            mtime: Some(Timestamp {
                seconds: 1000,
                nanos: 0,
            }),
        };
        let dir_dst = MyDirEntry {
            permissions: 0o700,
            ..dir_src.clone()
        };
        let diff_dir = DiffEntry {
            rel_path: "dir".to_string(),
            entries: vec![Some(dir_src.clone()), Some(dir_dst)],
            diff: crate::diff::DiffType::PERMISSIONS,
            sync_source_index: Some(0),
        };

        // 2. Symlink with different target
        let link_src = MyDirEntry {
            file_name: "link".to_string(),
            specific: Some(Specific::Symlink("target1".to_string())),
            permissions: 0o777,
            uid: 1000,
            gid: 1000,
            mtime: Some(Timestamp {
                seconds: 1000,
                nanos: 0,
            }),
        };
        let link_dst = MyDirEntry {
            specific: Some(Specific::Symlink("target2".to_string())),
            ..link_src.clone()
        };
        let diff_link = DiffEntry {
            rel_path: "link".to_string(),
            entries: vec![Some(link_src.clone()), Some(link_dst)],
            diff: crate::diff::DiffType::CONTENT,
            sync_source_index: Some(0),
        };

        let diff_entries = vec![diff_dir, diff_link];

        let handle =
            tokio::spawn(async move { sync_exec(task_tracker, &mut trees, &diff_entries).await });

        // Expect updates on tree2

        // 1. Update dir
        let req = t2_req_rx.recv_async().await?;
        if let ActionReq::CreateUpdateMetadata(req) = &*req {
            assert_eq!(req.rel_path, "dir");
            assert_eq!(req.metadata.as_ref().unwrap().permissions, 0o755);
        } else {
            panic!("Expected CreateUpdateMetadata for dir, got {req:?}");
        }

        // 2. Update link
        let req = t2_req_rx.recv_async().await?;
        if let ActionReq::CreateUpdateMetadata(req) = &*req {
            assert_eq!(req.rel_path, "link");
            if let Some(Specific::Symlink(target)) = &req.metadata.as_ref().unwrap().specific {
                assert_eq!(target, "target1");
            } else {
                panic!("Expected Symlink metadata");
            }
        } else {
            panic!("Expected CreateUpdateMetadata for link, got {req:?}");
        }

        // Expect EndMarker on tree1
        let req = t1_req_rx.recv_async().await?;
        if let ActionReq::EndMarker(_) = &*req {
            t1_rsp_tx
                .send_async(crate::proto::ActionRsp::EndMarker(PROTO_NULL_VALUE))
                .await?;
        } else {
            panic!("Expected EndMarker on tree1, got {req:?}");
        }

        // Expect EndMarker on tree2
        let req = t2_req_rx.recv_async().await?;
        if let ActionReq::EndMarker(_) = &*req {
            t2_rsp_tx
                .send_async(crate::proto::ActionRsp::EndMarker(PROTO_NULL_VALUE))
                .await?;
        } else {
            panic!("Expected EndMarker on tree2, got {req:?}");
        }

        handle.await??;

        task_tracker_main.request_stop();
        task_tracker_main.wait().await?;
        Ok(())
    }
}
