//! Manage one `Tree` on the local machine

use std::path::PathBuf;
use std::sync::Arc;

use flume::{Receiver, Sender};
use prost_types::Timestamp;

use crate::config::{ConfigRef, FileMatcher};
use crate::generic::file::FsTree;
use crate::generic::fs::MessageExt as _;
use crate::generic::task_tracker::{TaskExit, TaskTracker};
use crate::proto::{
    ActionReq, ActionRsp, MetadataSnap, MyDirEntry, MyDirEntryExt as _, Specific,
    get_metadata_snap_path,
};
use crate::tree::{Tree, TreeMetadata};

/// State for metadata
enum LocalMetadataState {
    /// Walking of the directory is on-going, handler to get the result
    Processing(Receiver<MyDirEntry>),
    /// Result is already available
    Received(MyDirEntry),
    /// Result has been consumed
    Terminated,
}

/// Content of one directory
struct DirContent {
    /// relative path of directory to root directory, "." if root
    pub rel_path: String,
    /// directory content, unsorted
    pub entries: Vec<MyDirEntry>,
}

/// Manage one `Tree` on the local machine
pub struct TreeLocal {
    config: ConfigRef,
    metadata_path: PathBuf,
    ts: Timestamp,
    fs_tree: Arc<FsTree>,
    metadata_state: LocalMetadataState,
}

impl TreeLocal {
    /// Spawn the walking of a local directory
    ///
    /// # Errors
    /// - Returns error if the directory cannot be walked
    pub fn spawn(
        config: ConfigRef,
        task_tracker: &TaskTracker,
        path: &str,
        ts: Timestamp,
        file_matcher: Option<FileMatcher>,
    ) -> anyhow::Result<Self> {
        let fs_tree = Arc::new(FsTree::new(path)?);
        let metadata_path = get_metadata_snap_path(&config, path);

        // content will finish in memory anyway, no need for flow control
        let (sender_content, receiver_content) = flume::unbounded();

        // expecting a single result
        let (sender_snap, receiver_snap) = flume::bounded(1);

        task_tracker.spawn_blocking({
            let fs_tree = fs_tree.clone();
            move || Self::walk_task(fs_tree, sender_content, file_matcher)
        })?;
        task_tracker.spawn(Self::hash_task(
            fs_tree.clone(),
            metadata_path.clone(),
            receiver_content,
            sender_snap,
        ))?;

        Ok(Self {
            config,
            metadata_path,
            ts,
            fs_tree,
            metadata_state: LocalMetadataState::Processing(receiver_snap),
        })
    }

    /// Task to walk the tree
    /// 1st step
    fn walk_task(
        fs_tree: Arc<FsTree>,
        sender_content: Sender<DirContent>,
        file_matcher: Option<FileMatcher>,
    ) -> anyhow::Result<TaskExit> {
        let mut dir_stack = vec![String::from(".")];

        while let Some(rel_path) = dir_stack.pop() {
            log::debug!("walk[{fs_tree}]: entering {rel_path}");

            let entries = fs_tree.walk_dir(&rel_path, |p| {
                if let Some(fm) = &file_matcher {
                    fm.is_ignored(p)
                } else {
                    false
                }
            })?;

            for e in entries.iter().rev() {
                if e.is_dir() {
                    dir_stack.push(rel_path.clone() + "/" + &e.file_name);
                }
            }
            sender_content.send(DirContent { rel_path, entries })?;
        }
        log::debug!("walk[{fs_tree}]: completed");
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }

    /// Task to add file hashes, from previous metadata snapshot if available
    /// 2nd step
    async fn hash_task(
        fs_tree: Arc<FsTree>,
        metadata_path: PathBuf,
        receiver_content: Receiver<DirContent>,
        sender_snap: Sender<MyDirEntry>,
    ) -> anyhow::Result<TaskExit> {
        let mut prev_snap = MetadataSnap::load_from_file(metadata_path).ok();
        let mut snap = fs_tree.stat(".")?;

        while let Ok(mut input) = receiver_content.recv_async().await {
            log::debug!("hash[{fs_tree}]: entering {}", input.rel_path);
            let mut prev_snap_dir = prev_snap
                .as_mut()
                .and_then(|prev_snap| prev_snap.get_entry_mut(&input.rel_path))
                .filter(|e| e.is_dir());

            // add hash of files
            for f in &mut input.entries {
                if let Some(Specific::Regular(file_data)) = &mut f.specific
                    && file_data.size != 0
                {
                    /* try to get hash from prev_snap_dir
                     * reuse if
                     * - same name
                     * - same mtime
                     * - same size
                     */
                    let prev_entry = prev_snap_dir
                        .as_mut()
                        .and_then(|s| s.get_entry_mut(&f.file_name));
                    let prev_file_hash = prev_entry.and_then(|prev_entry| {
                        if f.mtime == prev_entry.mtime
                            && let Some(Specific::Regular(prev_file_data)) =
                                &mut prev_entry.specific
                            && file_data.size == prev_file_data.size
                        {
                            Some(&mut prev_file_data.hash)
                        } else {
                            None
                        }
                    });

                    let rel_path = input.rel_path.clone() + "/" + &f.file_name;
                    if let Some(prev_file_hash) = prev_file_hash {
                        // steal hash from prev_snap, will not be reused anyway
                        log::debug!("hash[{fs_tree}]: reusing hash of {rel_path}",);
                        file_data.hash = std::mem::take(prev_file_hash);
                    } else {
                        // compute hash
                        log::debug!("hash[{fs_tree}]: compute hash of {rel_path}",);
                        let f = fs_tree.open(&rel_path)?;
                        file_data.hash = f.compute_hash()?.as_bytes().into();
                    }
                }
            }

            // add in collection
            snap.insert(&input.rel_path, input.entries)?;
        }

        log::debug!("hash[{fs_tree}]: completed");
        sender_snap.send(snap)?;
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }
}

// TODO: impl or Deref ?
impl TreeMetadata for TreeLocal {
    fn get_entry(&self, _rel_path: &str) -> Option<&MyDirEntry> {
        todo!()
    }

    fn get_dir_content(&self, _rel_path: &str) -> &[MyDirEntry] {
        todo!()
    }
}

#[async_trait::async_trait]
impl Tree for TreeLocal {
    async fn wait_for_tree(&mut self) -> anyhow::Result<()> {
        if let LocalMetadataState::Processing(receiver_snap) = &self.metadata_state {
            let snap = receiver_snap.recv_async().await?;
            self.metadata_state = LocalMetadataState::Received(snap);
        }
        Ok(())
    }

    fn get_fs_action_requester(&self) -> Sender<ActionReq> {
        todo!();
    }

    fn get_fs_action_responder(&self) -> Receiver<ActionRsp> {
        todo!();
    }
}

impl Drop for TreeLocal {
    fn drop(&mut self) {
        if let LocalMetadataState::Received(snap) =
            std::mem::replace(&mut self.metadata_state, LocalMetadataState::Terminated)
        {
            // save metadata
            let snap = MetadataSnap {
                ts: Some(self.ts),
                root: Some(snap),
            };
            drop(snap.save_to_file(&self.metadata_path));
        }
    }
}
