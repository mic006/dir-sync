//! Manage one `Tree` on the local machine

use rayon::prelude::*;

use std::path::PathBuf;
use std::sync::Arc;

use flume::{Receiver, Sender};
use prost_types::Timestamp;

use crate::config::{ConfigRef, FileMatcher};
use crate::generic::file::FsTree;
use crate::generic::fs::MessageExt as _;
use crate::generic::task_tracker::{TaskExit, TaskTracker};
use crate::proto::{
    ActionReq, ActionRsp, DirectoryData, MetadataSnap, MyDirEntry, MyDirEntryExt as _, Specific,
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

/// Context shared by all nodes
struct NodeSharedCtx {
    fs_tree: Arc<FsTree>,
    file_matcher: Option<FileMatcher>,
    // TODO: add task_tracker to allow early exit
}

fn recursive_walk(
    ctx: &Arc<NodeSharedCtx>,
    rel_path: String,
    mut prev_snap: Option<MyDirEntry>,
) -> anyhow::Result<Vec<MyDirEntry>> {
    // get directory content, filtering out ignored files
    let mut entries = ctx.fs_tree.walk_dir(&rel_path, |p| {
        if let Some(fm) = &ctx.file_matcher {
            fm.is_ignored(p)
        } else {
            false
        }
    })?;
    entries.sort_by(MyDirEntry::cmp);

    // serialized: get sub directories and their associated previous snapshot, if any
    let subdirs = entries
        .iter_mut()
        .filter_map(|e| {
            if e.is_dir() {
                let prev_snap_subdir = prev_snap
                    .as_ref()
                    .and_then(|prev_snap| prev_snap.get_entry(&e.file_name).cloned()); //TODO: take instead of clone
                Some((e, prev_snap_subdir))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // parallelized: get and update sub directories content recursively
    subdirs.into_par_iter().try_for_each_with(
        ctx.clone(),
        |ctx, (e, prev_snap)| -> anyhow::Result<()> {
            if e.is_dir() {
                let entries =
                    recursive_walk(ctx, format!("{rel_path}/{}", e.file_name), prev_snap)?;
                e.specific = Some(Specific::Directory(crate::proto::DirectoryData {
                    content: entries,
                }));
            }
            Ok(())
        },
    )?;

    // serialized: update file hash from previous snapshot, if any
    // return the files to be hashed
    let files_to_hash = entries
        .iter_mut()
        .filter_map(|e| {
            if let Some(Specific::Regular(file_data)) = &mut e.specific
                && file_data.size != 0
            {
                /* try to get hash from prev_snap_dir
                 * reuse if
                 * - same name
                 * - same mtime
                 * - same size
                 */
                let prev_entry = prev_snap
                    .as_mut()
                    .and_then(|prev_snap| prev_snap.get_entry_mut(&e.file_name));
                let prev_file_hash = prev_entry.and_then(|prev_entry| {
                    if e.mtime == prev_entry.mtime
                        && let Some(Specific::Regular(prev_file_data)) = &mut prev_entry.specific
                        && file_data.size == prev_file_data.size
                    {
                        Some(&mut prev_file_data.hash)
                    } else {
                        None
                    }
                });
                if let Some(prev_file_hash) = prev_file_hash {
                    // steal hash from prev_snap, will not be reused anyway
                    log::debug!("hash[{}]: reusing hash of {rel_path}", ctx.fs_tree);
                    file_data.hash = std::mem::take(prev_file_hash);
                    // hash retrieved from previous snapshot, skip
                    None
                } else {
                    // compute hash
                    Some(e)
                }
            } else {
                // not a regular file or empty file, skip
                None
            }
        })
        .collect::<Vec<_>>();

    // parallelized: compute hash of files
    files_to_hash.into_par_iter().try_for_each_with(
        ctx.clone(),
        |ctx, e| -> anyhow::Result<()> {
            let rel_path = format!("{rel_path}/{}", e.file_name);
            log::debug!("hash[{}]: compute hash of {rel_path}", ctx.fs_tree);
            let Some(Specific::Regular(file_data)) = &mut e.specific else {
                unreachable!("working only on regular files")
            };
            let f = ctx.fs_tree.open(&rel_path)?;
            file_data.hash = f.compute_hash()?.as_bytes().into();
            Ok(())
        },
    )?;

    Ok(entries)
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

        // expecting a single result
        let (sender_snap, receiver_snap) = flume::bounded(1);

        task_tracker.spawn_blocking({
            let task_tracker = task_tracker.clone();
            let fs_tree = fs_tree.clone();
            let metadata_path = metadata_path.clone();
            move || {
                Self::walk_task(
                    task_tracker,
                    fs_tree,
                    file_matcher,
                    metadata_path,
                    sender_snap,
                )
            }
        })?;

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
        _task_tracker: TaskTracker,
        fs_tree: Arc<FsTree>,
        file_matcher: Option<FileMatcher>,
        metadata_path: PathBuf,
        sender_snap: Sender<MyDirEntry>,
    ) -> anyhow::Result<TaskExit> {
        log::info!("walk[{fs_tree}]: starting");
        let prev_snap = MetadataSnap::load_from_file(metadata_path).ok();
        let prev_snap = prev_snap.and_then(|snap| snap.root);

        let mut snap = fs_tree.stat(".")?;

        let ctx = Arc::new(NodeSharedCtx {
            fs_tree: fs_tree.clone(),
            file_matcher,
        });

        let entries = recursive_walk(&ctx, String::from("."), prev_snap)?;
        snap.specific = Some(Specific::Directory(DirectoryData { content: entries }));

        log::info!("walk[{fs_tree}]: completed");
        sender_snap.send(snap)?;
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }
}

impl TreeMetadata for TreeLocal {
    fn get_entry(&self, rel_path: &str) -> Option<&MyDirEntry> {
        let LocalMetadataState::Received(snap) = &self.metadata_state else {
            panic!("inconsistent state, call wait_for_tree() first");
        };
        snap.get_entry(rel_path)
    }

    fn get_dir_content(&self, rel_path: &str) -> &[MyDirEntry] {
        let Some(entry) = self.get_entry(rel_path) else {
            return &[];
        };
        let Some(Specific::Directory(dir_data)) = &entry.specific else {
            return &[];
        };
        &dir_data.content
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

#[cfg(test)]
mod tests {
    use crate::config::tests::load_ut_cfg;
    use crate::generic::file::MyHash;
    use crate::generic::fs::PathExt as _;
    use crate::generic::task_tracker::TaskTrackerMain;
    use crate::proto::TimestampExt as _;

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_tree_local_walk_hash() -> anyhow::Result<()> {
        crate::generic::test::log_init();

        // create test dir with some content
        let test_dir = tempfile::tempdir()?;
        let test_dir_path = test_dir.path();
        for p in [
            "sub_folder1/empty",
            "sub_folder1/bar",
            "sub folder 2 €",
            "empty_folder",
        ] {
            std::fs::create_dir_all(test_dir_path.join(p))?;
        }
        std::fs::write(test_dir_path.join("some file"), "alpha")?;
        std::fs::write(test_dir_path.join("empty file"), "")?;
        std::fs::write(test_dir_path.join("sub folder 2 €/beta"), "some data")?;
        std::fs::write(test_dir_path.join("sub_folder1/bar/ignored"), "ignored")?;
        std::fs::write(test_dir_path.join("sub_folder1/ignored~"), "ignored")?;
        std::os::unix::fs::symlink("target", test_dir_path.join("some_link"))?;

        let config = Arc::new(load_ut_cfg().unwrap());
        let ts = Timestamp::now();
        let file_matcher = config.get_file_matcher(Some("data")).unwrap();

        // walk the directory
        let mut task_tracker_main = TaskTrackerMain::default();
        let task_tracker = task_tracker_main.tracker();
        let mut tree = TreeLocal::spawn(
            config,
            &task_tracker,
            test_dir_path.checked_as_str()?,
            ts,
            file_matcher,
        )?;
        drop(task_tracker);

        tree.wait_for_tree().await?;

        {
            let root_content = tree.get_dir_content(".");
            //println!("{root_content:#?}");
            assert_eq!(root_content.len(), 6);
            let file_names: Vec<_> = root_content.iter().map(|e| e.file_name.as_str()).collect();
            assert_eq!(
                file_names,
                vec![
                    "empty file",
                    "empty_folder",
                    "some file",
                    "some_link",
                    "sub folder 2 €",
                    "sub_folder1"
                ]
            );
        }
        {
            let sub_content = tree.get_dir_content("./sub folder 2 €");
            //println!("{sub_content:#?}");
            assert_eq!(sub_content.len(), 1);
            assert_eq!(sub_content[0].file_name, "beta");
        }
        {
            let entry = tree.get_entry("./sub folder 2 €/beta").unwrap();
            assert_eq!(entry.file_name, "beta");
            let Some(Specific::Regular(file_data)) = &entry.specific else {
                panic!("beta file error");
            };
            assert_eq!(file_data.size, 9);
            assert_eq!(
                MyHash::from_slice(&file_data.hash)?,
                MyHash::from_hex(
                    "b224a1da2bf5e72b337dc6dde457a05265a06dec8875be379e2ad2be5edb3bf2"
                )?
            );
        }
        {
            let entry = tree.get_entry("./some_link").unwrap();
            let Some(Specific::Symlink(symlink_data)) = &entry.specific else {
                panic!("some_link error");
            };
            assert_eq!(symlink_data, "target");
        }

        task_tracker_main.request_stop();
        task_tracker_main.wait().await?;
        Ok(())
    }
}
