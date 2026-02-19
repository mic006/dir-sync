//! Manage one `Tree` on the local machine

use rayon::prelude::*;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use flume::{Receiver, Sender};
use prost_types::Timestamp;

use crate::config::{ConfigRef, FileMatcher};
use crate::diff::{self, DiffType};
use crate::generic::file::{FsFile, FsTree};
use crate::generic::fs::PathExt as _;
use crate::generic::task_tracker::{TaskExit, TaskTracker};
use crate::proto::action::FileReadRsp;
use crate::proto::{
    ActionReq, ActionRsp, DirectoryData, MetadataSnap, MyDirEntry, MyDirEntryExt as _,
    PROTO_NULL_VALUE, Specific,
    action::{DeleteEntryReq, ErrorRsp, FileReadReq, FileWriteReq, UpdateMetadataReq},
};
use crate::snap::SnapAccess;
use crate::tree::{ActionReqSender, ActionRspReceiver, Tree, TreeMetadata};

/// Temporary file used by dir-sync to update a file
///
/// The file is firstly created using this temp name, updated with data + metadata,
/// then renamed to its final name (potentially overwriting an existing file atomically)
const TEMP_FILE: &str = ".dir_sync.tmp";

/// State for metadata
enum LocalMetadataState {
    /// Walking of the directory is on-going, handler to get the result
    Processing(Receiver<Box<WalkOutput>>),
    /// Result is already available
    Received(Box<WalkOutput>),
    /// Result has been consumed
    Terminated,
}

/// Output of the walk task
struct WalkOutput {
    /// Metadata snapshot of the tree
    snap: MyDirEntry,
    /// Previous sync snapshot for the tree, if any
    prev_sync_snap: Option<MetadataSnap>,
}

/// Context for walking the tree
struct WalkCtx {
    fs_tree: Arc<FsTree>,
    file_matcher: Option<FileMatcher>,
    task_tracker: TaskTracker,
}

impl WalkCtx {
    /// Walk tree recursively
    fn walk_recursive(
        &self,
        rel_path: String,
        current_entry: &mut MyDirEntry,
        mut prev_snap: Option<MyDirEntry>,
    ) -> anyhow::Result<()> {
        self.task_tracker.get_shutdown_req_as_result()?;
        log::debug!("tree[{}]: entering {rel_path}", self.fs_tree);

        let mut entries = self.fs_tree.walk_dir(&rel_path, |p| self.ignore(p))?;
        // delete the tempfile left over from a previous sync
        entries.retain(|e| {
            if e.file_name == TEMP_FILE {
                let _ignored = self.fs_tree.remove_file(&format!("{rel_path}/{TEMP_FILE}"));
                false
            } else {
                true
            }
        });
        entries.sort_by(MyDirEntry::cmp);

        let mut subdirs = Vec::with_capacity(entries.len());
        let mut files_to_hash = Vec::with_capacity(entries.len());

        // process all entries
        // - determine sub directories to be walked recursively, collected in subdirs
        // - handle regular files hash
        //   - reuse hash from prev_snap if available and matching
        //   - collect remaining files in files_to_hash
        for e in &mut entries {
            if e.is_dir() {
                let prev_snap_subdir = prev_snap
                    .as_mut()
                    .and_then(|prev_snap| prev_snap.take_entry(&e.file_name));
                subdirs.push((e, prev_snap_subdir));
            } else if let Some(Specific::Regular(file_data)) = &mut e.specific
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
                    log::debug!("tree[{}]: reusing hash of {rel_path}", self.fs_tree);
                    file_data.hash = std::mem::take(prev_file_hash);
                    // hash retrieved from previous snapshot, skip
                } else {
                    // need to compute hash
                    files_to_hash.push((format!("{rel_path}/{}", e.file_name), file_data));
                }
            }
        }

        // parallelized: process sub directories recursively
        subdirs
            .into_par_iter()
            .try_for_each(|(e, prev_snap)| -> anyhow::Result<()> {
                self.walk_recursive(format!("{rel_path}/{}", e.file_name), e, prev_snap)
            })?;

        // parallelized: compute hash of files
        files_to_hash.into_par_iter().try_for_each(
            |(rel_path, file_data)| -> anyhow::Result<()> {
                self.task_tracker.get_shutdown_req_as_result()?;
                log::debug!("tree[{}]: compute hash of {rel_path}", self.fs_tree);
                let f = self.fs_tree.open(&rel_path)?;
                file_data.hash = f.compute_hash()?.as_bytes().into();
                Ok(())
            },
        )?;

        // update directory content
        current_entry.specific = Some(Specific::Directory(DirectoryData { content: entries }));

        log::debug!("tree[{}]: leaving {rel_path}", self.fs_tree);
        Ok(())
    }

    /// Indicate if relative path shall be ignored during walking
    fn ignore(&self, rel_path: &str) -> bool {
        if let Some(fm) = &self.file_matcher {
            fm.is_ignored(rel_path)
        } else {
            false
        }
    }
}

/// Context tracking a regular file being created
struct OngoingWriteFile {
    /// File handle
    f: FsFile,
    /// Current offset
    offset: u64,
    /// Target relative path
    rel_path: String,
}

/// Context to execute filesystem actions
struct ActionCtx {
    fs_tree: Arc<FsTree>,
    receiver: Receiver<Arc<ActionReq>>,
    sender: Sender<ActionRsp>,
    read_buf_size: usize,
    ongoing_write_file: Option<OngoingWriteFile>,
}
impl ActionCtx {
    async fn task(&mut self) -> anyhow::Result<TaskExit> {
        while let Ok(req) = self.receiver.recv_async().await {
            let res = match &*req {
                ActionReq::EndMarker(_) => {
                    self.sender
                        .send_async(ActionRsp::EndMarker(PROTO_NULL_VALUE))
                        .await?;
                    Ok(())
                }
                ActionReq::DeleteFile(req) => {
                    self.delete_file(req).map_err(|err| (&req.rel_path, err))
                }
                ActionReq::DeleteDir(req) => {
                    self.delete_dir(req).map_err(|err| (&req.rel_path, err))
                }
                ActionReq::CreateUpdateMetadata(req) => self
                    .create_update_metadata(req)
                    .map_err(|err| (&req.rel_path, err)),
                ActionReq::CreateUpdateFile(req) => {
                    self.create_update_file(req).await.map_err(|err| {
                        // for any error, drop the ongoing file
                        self.ongoing_write_file = None;
                        (&req.rel_path, err)
                    })
                }
                ActionReq::ReadFile(req) => self
                    .read_file(req)
                    .await
                    .map_err(|err| (&req.rel_path, err)),
            };
            if let Err((rel_path, err)) = res {
                self.sender
                    .send_async(ActionRsp::Error(ErrorRsp {
                        rel_path: rel_path.clone(),
                        message: err.to_string(),
                    }))
                    .await?;
            }
        }

        Ok(TaskExit::SecondaryTaskKeepRunning)
    }

    fn delete_file(&self, req: &DeleteEntryReq) -> anyhow::Result<()> {
        self.fs_tree.remove_file(&req.rel_path)
    }

    fn delete_dir(&self, req: &DeleteEntryReq) -> anyhow::Result<()> {
        self.fs_tree.remove_dir(&req.rel_path)
    }

    fn create_update_metadata(&self, req: &UpdateMetadataReq) -> anyhow::Result<()> {
        let current = self.fs_tree.stat(&req.rel_path).ok();
        let new = req.metadata.as_ref().unwrap();
        let diff = current.map_or(DiffType::all(), |current| diff::diff_entries(&current, new));

        if diff.intersects(DiffType::TYPE | DiffType::CONTENT) {
            // create entry as tmp file, set metadata and rename it to final name

            let mut temp_file = PathBuf::from(&req.rel_path);
            temp_file.set_file_name(TEMP_FILE);
            let temp_file = temp_file.checked_as_str()?;
            match new.specific.as_ref().unwrap() {
                Specific::Directory(_) => {
                    self.fs_tree.mkdir(temp_file)?;
                    self.set_metadata_and_rename(temp_file, &req.rel_path, new)?;
                }
                Specific::Symlink(target) => {
                    self.fs_tree.symlink(temp_file, target)?;
                    self.set_metadata_and_rename(temp_file, &req.rel_path, new)?;
                }
                Specific::Regular(reg_data) => {
                    anyhow::ensure!(
                        reg_data.size == 0,
                        "internal error, create_update_metadata cannot create regular files with content"
                    );
                    let parent = Path::new(&req.rel_path)
                        .parent()
                        .unwrap()
                        .checked_as_str()?;
                    let f = self.fs_tree.create_tmp(parent, 0)?;
                    self.finalize_regular_file(f, &req.rel_path, new)?;
                }
                _ => anyhow::bail!("unsupported file type"),
            }
        } else {
            // update metadata only on the existing entry
            if diff.contains(DiffType::PERMISSIONS) {
                self.fs_tree.chmod(&req.rel_path, new.permissions)?;
            }
            if diff.contains(DiffType::UID_GID) {
                self.fs_tree.chown(&req.rel_path, new.uid, new.gid)?;
            }
            if !new.is_dir() && diff.contains(DiffType::MTIME) {
                self.fs_tree
                    .set_mtime(&req.rel_path, new.mtime.as_ref().unwrap())?;
            }
        }

        Ok(())
    }

    async fn create_update_file(&mut self, req: &FileWriteReq) -> anyhow::Result<()> {
        if req.offset == 0 {
            // first data segment, need to create the file
            if let Some(ongoing_write_file) = self.ongoing_write_file.take() {
                self.sender
                    .send_async(ActionRsp::Error(ErrorRsp {
                        rel_path: ongoing_write_file.rel_path,
                        message: "create_update_file: new file received while previous one has not been finalized".into(),
                    }))
                    .await?;
            }
            let parent = Path::new(&req.rel_path)
                .parent()
                .unwrap()
                .checked_as_str()?;
            let f = self.fs_tree.create_tmp(parent, req.size.unwrap())?;
            self.ongoing_write_file = Some(OngoingWriteFile {
                f,
                offset: 0,
                rel_path: req.rel_path.clone(),
            });
        } else {
            // subsequent segment - check consistency
            let Some(ongoing_write_file) = &self.ongoing_write_file else {
                anyhow::bail!("create_update_file: missing first segment to create file");
            };
            anyhow::ensure!(
                ongoing_write_file.rel_path == req.rel_path,
                "create_update_file: inconsistent relative path"
            );
            anyhow::ensure!(
                ongoing_write_file.offset == req.offset,
                "create_update_file: inconsistent file offset"
            );
        }

        let ongoing_write_file = self.ongoing_write_file.as_mut().unwrap();

        // write data
        ongoing_write_file.f.write(&req.data)?;
        ongoing_write_file.offset += req.data.len() as u64;

        if let Some(metadata) = &req.metadata {
            // last segment, finalize file
            let ongoing_write_file = self.ongoing_write_file.take().unwrap();

            // check file size and data checksum
            let Some(Specific::Regular(regular_data)) = &metadata.specific else {
                anyhow::bail!("create_update_file: invalid metadata");
            };
            let file_size = ongoing_write_file.f.file_size()?;
            anyhow::ensure!(
                file_size == regular_data.size,
                "create_update_file: inconsistent file size"
            );
            let hash = ongoing_write_file.f.compute_hash()?;
            anyhow::ensure!(
                hash.as_bytes() == &regular_data.hash[..],
                "create_update_file: inconsistent hash"
            );

            // finalize file
            self.finalize_regular_file(ongoing_write_file.f, &req.rel_path, metadata)?;
        }

        Ok(())
    }

    async fn read_file(&self, req: &FileReadReq) -> anyhow::Result<()> {
        let f = self.fs_tree.open(&req.rel_path)?;

        // read file content by segments
        let mut offset = 0;
        loop {
            let mut data = Vec::with_capacity(self.read_buf_size);
            let read = f.read(&mut data)?;
            if read == 0 {
                break; // end of file reached
            }
            // send segment
            self.sender
                .send_async(ActionRsp::FileData(FileReadRsp {
                    rel_path: req.rel_path.clone(),
                    offset,
                    data,
                }))
                .await?;
            offset += read;
        }

        Ok(())
    }

    /// Set all metadata on temp file and rename it to its final name
    fn set_metadata_and_rename(
        &self,
        temp_rel_path: &str,
        final_rel_path: &str,
        metadata: &MyDirEntry,
    ) -> anyhow::Result<()> {
        self.fs_tree.chmod(temp_rel_path, metadata.permissions)?;
        self.fs_tree
            .chown(temp_rel_path, metadata.uid, metadata.gid)?;
        self.fs_tree
            .set_mtime(temp_rel_path, metadata.mtime.as_ref().unwrap())?;
        self.fs_tree.rename(temp_rel_path, final_rel_path)?;
        Ok(())
    }

    /// Finalize regular file creation
    ///
    /// Set metadata, then commit file and rename it to its final name
    fn finalize_regular_file(
        &self,
        f: FsFile,
        final_rel_path: &str,
        metadata: &MyDirEntry,
    ) -> anyhow::Result<()> {
        // set metadata
        f.chmod(metadata.permissions)?;
        f.chown(metadata.uid, metadata.gid)?;
        f.set_mtime(metadata.mtime.as_ref().unwrap())?;

        // create file with temp name
        let mut temp_file = PathBuf::from(final_rel_path);
        temp_file.set_file_name(TEMP_FILE);
        let temp_file = temp_file.checked_as_str()?;
        self.fs_tree.commit_tmp(temp_file, f)?;

        // rename
        self.fs_tree.rename(temp_file, final_rel_path)?;
        Ok(())
    }
}

/// Manage one `Tree` on the local machine
pub struct TreeLocal {
    /// Configuration
    config: ConfigRef,
    /// Canonicalized path
    path: String,
    /// Access to metadata snapshots
    snap_access: SnapAccess,
    /// Timestamp of the current snapshot
    ts: Timestamp,
    /// Root fd
    fs_tree: Arc<FsTree>,
    /// Current state
    metadata_state: LocalMetadataState,
    /// Syncs retrieved from the previous metadata snapshot
    last_syncs: BTreeMap<String, Timestamp>,
    /// Other paths to perform synchronization with
    sync_paths: Arc<Vec<String>>,
}

impl TreeLocal {
    /// Spawn the walking of a local directory
    ///
    /// # Errors
    /// - Returns error if the directory cannot be walked
    pub fn spawn(
        config: ConfigRef,
        task_tracker: &TaskTracker,
        path: &str,                        // path to walk
        ts: Timestamp,                     // reference timestamp
        file_matcher: Option<FileMatcher>, // filter for files to walk
        sync_paths: Vec<String>,           // other paths to perform synchronization with
    ) -> anyhow::Result<Self> {
        let fs_tree = Arc::new(FsTree::new(path)?);

        let snap_access = SnapAccess::new(&config, path);

        // read previous snapshot
        let mut prev_snap = snap_access.load_main_snap();
        // keep last syncs to add in updated snapshot
        let last_syncs = prev_snap
            .as_mut()
            .map(|snap| std::mem::take(&mut snap.last_syncs))
            .unwrap_or_default();
        // extract snap content to speed up the new snap
        let prev_snap = prev_snap.and_then(|snap| snap.root);

        // expecting a single result
        let (sender_snap, receiver_snap) = flume::bounded(1);

        let sync_paths = Arc::new(sync_paths);

        task_tracker.spawn_blocking({
            let task_tracker = task_tracker.clone();
            let fs_tree = fs_tree.clone();
            let snap_access = snap_access.clone();
            let sync_paths = sync_paths.clone();
            move || {
                Self::walk_task(
                    task_tracker,
                    fs_tree,
                    file_matcher,
                    prev_snap,
                    sync_paths,
                    snap_access,
                    sender_snap,
                )
            }
        })?;

        Ok(Self {
            config,
            path: path.into(),
            snap_access,
            ts,
            fs_tree,
            metadata_state: LocalMetadataState::Processing(receiver_snap),
            last_syncs,
            sync_paths,
        })
    }

    /// Task to walk the tree
    fn walk_task(
        task_tracker: TaskTracker,
        fs_tree: Arc<FsTree>,
        file_matcher: Option<FileMatcher>,
        prev_snap: Option<MyDirEntry>,
        sync_paths: Arc<Vec<String>>, // other paths to perform synchronization with
        snap_access: SnapAccess,
        sender_snap: Sender<Box<WalkOutput>>,
    ) -> anyhow::Result<TaskExit> {
        log::info!("tree[{fs_tree}]: starting walk");

        let mut snap = fs_tree.stat(".")?;

        let ctx = WalkCtx {
            fs_tree: fs_tree.clone(),
            file_matcher,
            task_tracker,
        };

        ctx.walk_recursive(String::from("."), &mut snap, prev_snap)?;

        log::info!("tree[{fs_tree}]: walk completed");

        let prev_sync_snap = (!sync_paths.is_empty())
            .then(|| {
                let prev_sync_snap = snap_access.load_common_sync_snap(&sync_paths);
                log::info!(
                    "tree[{fs_tree}]: previous sync snapshot {}",
                    if prev_sync_snap.is_some() {
                        "loaded"
                    } else {
                        "not found"
                    }
                );
                prev_sync_snap
            })
            .flatten();

        sender_snap.send(Box::new(WalkOutput {
            snap,
            prev_sync_snap,
        }))?;
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }
}

impl TreeMetadata for TreeLocal {
    fn get_entry(&self, rel_path: &str) -> Option<&MyDirEntry> {
        let LocalMetadataState::Received(output) = &self.metadata_state else {
            panic!("inconsistent state, call wait_for_tree() first");
        };
        output.snap.get_entry(rel_path)
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

    fn get_fs_action_requester(&self) -> ActionReqSender {
        todo!();
    }

    fn get_fs_action_responder(&self) -> ActionRspReceiver {
        todo!();
    }

    fn save_snap(&mut self, sync: bool) {
        if let LocalMetadataState::Received(output) =
            std::mem::replace(&mut self.metadata_state, LocalMetadataState::Terminated)
        {
            log::info!("tree[{}]: saving snap", self.fs_tree);
            // restore last_syncs from loaded snapshot
            let mut last_syncs = std::mem::take(&mut self.last_syncs);
            if sync {
                // and update with synced remotes
                for sync_path in self.sync_paths.iter() {
                    last_syncs.insert(sync_path.clone(), self.ts);
                }
            }
            // save metadata
            let snap = MetadataSnap {
                ts: Some(self.ts),
                path: self.path.clone(),
                last_syncs,
                root: Some(output.snap),
            };
            let synced_remotes = if sync {
                self.sync_paths
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            } else {
                vec![]
            };
            match self.snap_access.save_snap(&snap, &synced_remotes) {
                Ok(()) => log::info!("tree[{}]: snap saved", self.fs_tree),
                Err(err) => log::warn!("tree[{}]: cannot save snap: {err}", self.fs_tree),
            }
        }
    }

    fn take_prev_sync_snap(&mut self) -> Option<MetadataSnap> {
        if let LocalMetadataState::Received(output) = &mut self.metadata_state {
            output.prev_sync_snap.take()
        } else {
            None
        }
    }
}

impl Drop for TreeLocal {
    fn drop(&mut self) {
        // ensure a completed snapshot is saved
        self.save_snap(false);
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
            vec![],
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
