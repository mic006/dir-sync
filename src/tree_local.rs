//! Manage one `Tree` on the local machine

use rayon::prelude::*;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use flume::{Receiver, Sender};
use prost_types::Timestamp;

use crate::config::ConfigRef;
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
use crate::tree::{ActionReqSender, ActionRspReceiver, Tree, TreeMetadataState, TreeWalkOutput};

/// Temporary file used by dir-sync to update a file
///
/// The file is firstly created using this temp name, updated with data + metadata,
/// then renamed to its final name (potentially overwriting an existing file atomically)
const TEMP_FILE: &str = ".dir_sync.tmp";

/// Context for walking the tree
struct WalkCtx {
    config: ConfigRef,
    fs_tree: Arc<FsTree>,
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
        if let Some(fm) = &self.config.file_matcher {
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
    snap: Arc<Mutex<MyDirEntry>>,
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
                    log::debug!("tree[{}]: action EndMarker", self.fs_tree);
                    self.sender
                        .send_async(ActionRsp::EndMarker(PROTO_NULL_VALUE))
                        .await?;
                    Ok(())
                }
                ActionReq::DeleteFile(req) => {
                    log::debug!(
                        "tree[{}]: action DeleteFile({})",
                        self.fs_tree,
                        req.rel_path
                    );
                    self.delete_file(req).map_err(|err| (&req.rel_path, err))
                }
                ActionReq::DeleteDir(req) => {
                    log::debug!("tree[{}]: action DeleteDir({})", self.fs_tree, req.rel_path);
                    self.delete_dir(req).map_err(|err| (&req.rel_path, err))
                }
                ActionReq::CreateUpdateMetadata(req) => {
                    log::debug!(
                        "tree[{}]: action CreateUpdateMetadata({})",
                        self.fs_tree,
                        req.rel_path
                    );
                    self.create_update_metadata(req)
                        .map_err(|err| (&req.rel_path, err))
                }
                ActionReq::CreateUpdateFile(req) => {
                    log::debug!(
                        "tree[{}]: action CreateUpdateFile({})",
                        self.fs_tree,
                        req.rel_path
                    );
                    self.create_update_file(req).await.map_err(|err| {
                        // for any error, drop the ongoing file
                        self.ongoing_write_file = None;
                        (&req.rel_path, err)
                    })
                }
                ActionReq::ReadFile(req) => {
                    log::debug!("tree[{}]: action ReadFile({})", self.fs_tree, req.rel_path);
                    self.read_file(req)
                        .await
                        .map_err(|err| (&req.rel_path, err))
                }
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
        self.fs_tree.remove_file(&req.rel_path)?;
        self.snap.lock().unwrap().delete_entry(&req.rel_path);
        Ok(())
    }

    fn delete_dir(&self, req: &DeleteEntryReq) -> anyhow::Result<()> {
        self.fs_tree.remove_dir(&req.rel_path)?;
        self.snap.lock().unwrap().delete_entry(&req.rel_path);
        Ok(())
    }

    fn create_update_metadata(&self, req: &UpdateMetadataReq) -> anyhow::Result<()> {
        let current = self.fs_tree.stat(&req.rel_path).ok();
        let new = req.metadata.as_ref().unwrap();
        let diff = current.map_or(DiffType::all(), |current| diff::diff_entries(&current, new));

        // IMPORTANT: for a regular file, current does not contain file hash
        // so CONTENT bit shall be ignored for regular files
        if diff.contains(DiffType::TYPE) || (diff.contains(DiffType::CONTENT) && !new.is_file()) {
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

        // update entry metadata in snap
        let entry = self.fs_tree.stat(&req.rel_path)?;
        self.snap
            .lock()
            .unwrap()
            .create_or_update_entry(&req.rel_path, entry)?;

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
            let hash = hash.as_bytes().to_vec();
            anyhow::ensure!(
                hash == regular_data.hash,
                "create_update_file: inconsistent hash"
            );

            // finalize file
            self.finalize_regular_file(ongoing_write_file.f, &req.rel_path, metadata)?;

            // update entry metadata in snap
            let mut entry = self.fs_tree.stat(&req.rel_path)?;
            // add hash
            let Some(Specific::Regular(regular_data)) = &mut entry.specific else {
                anyhow::bail!("create_update_file: invalid metadata for newly created file");
            };
            regular_data.hash = hash;
            self.snap
                .lock()
                .unwrap()
                .create_or_update_entry(&req.rel_path, entry)?;
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
        if !metadata.is_symlink() {
            self.fs_tree.chmod(temp_rel_path, metadata.permissions)?;
        }
        self.fs_tree
            .chown(temp_rel_path, metadata.uid, metadata.gid)?;
        if !metadata.is_dir() {
            self.fs_tree
                .set_mtime(temp_rel_path, metadata.mtime.as_ref().unwrap())?;
        }
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

struct ActionDeps {
    task_tracker: TaskTracker,
    receiver_act_req: Receiver<Arc<ActionReq>>,
    sender_act_rsp: Sender<ActionRsp>,
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
    /// Dependencies to launch `ActionCtx`
    action_deps: Option<ActionDeps>,
    /// Action requester
    sender_act_req: ActionReqSender,
    /// Action responder
    receiver_act_rsp: ActionRspReceiver,
    /// Current state
    metadata_state: TreeMetadataState,
    /// Syncs retrieved from the previous metadata snapshot
    last_syncs: BTreeMap<String, Timestamp>,
    /// Other paths to perform synchronization with
    sync_fqns: Arc<Vec<String>>,
}

impl TreeLocal {
    /// Spawn the walking of a local directory
    ///
    /// # Errors
    /// - Returns error if the directory cannot be walked
    pub fn spawn(
        config: ConfigRef,
        task_tracker: &TaskTracker,
        path: &str,             // path to walk
        ts: Timestamp,          // reference timestamp
        sync_fqns: Vec<String>, // other paths to perform synchronization with
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

        let sync_fqns = Arc::new(sync_fqns);

        task_tracker.spawn_blocking({
            let config = config.clone();
            let task_tracker = task_tracker.clone();
            let fs_tree = fs_tree.clone();
            let snap_access = snap_access.clone();
            let sync_fqns = sync_fqns.clone();
            move || {
                Self::walk_task(
                    config,
                    task_tracker,
                    fs_tree,
                    prev_snap,
                    sync_fqns,
                    snap_access,
                    sender_snap,
                )
            }
        })?;

        let (sender_act_req, receiver_act_req) = flume::bounded(config.perf_fs_queue_size);
        let (sender_act_rsp, receiver_act_rsp) = flume::bounded(config.perf_fs_queue_size);

        Ok(Self {
            config,
            path: path.into(),
            snap_access,
            ts,
            fs_tree,
            action_deps: Some(ActionDeps {
                task_tracker: task_tracker.clone(),
                receiver_act_req,
                sender_act_rsp,
            }),
            sender_act_req,
            receiver_act_rsp,
            metadata_state: TreeMetadataState::Processing(receiver_snap),
            last_syncs,
            sync_fqns,
        })
    }

    /// Task to walk the tree
    fn walk_task(
        config: ConfigRef,
        task_tracker: TaskTracker,
        fs_tree: Arc<FsTree>,
        prev_snap: Option<MyDirEntry>,
        sync_fqns: Arc<Vec<String>>, // other paths to perform synchronization with
        snap_access: SnapAccess,
        sender_snap: Sender<TreeWalkOutput>,
    ) -> anyhow::Result<TaskExit> {
        log::info!("tree[{fs_tree}]: starting walk");

        let mut snap = fs_tree.stat(".")?;

        let ctx = WalkCtx {
            config,
            fs_tree: fs_tree.clone(),
            task_tracker,
        };

        ctx.walk_recursive(String::from("."), &mut snap, prev_snap)?;

        log::info!("tree[{fs_tree}]: walk completed");

        let prev_sync_snap = (!sync_fqns.is_empty())
            .then(|| {
                let prev_sync_snap = snap_access.load_common_sync_snap(&sync_fqns);
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

        sender_snap.send(TreeWalkOutput {
            snap: Arc::new(Mutex::new(snap)),
            prev_sync_snap,
        })?;
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }
}

#[async_trait::async_trait]
impl Tree for TreeLocal {
    async fn wait_for_tree(&mut self) -> anyhow::Result<()> {
        if let TreeMetadataState::Processing(receiver_snap) = &self.metadata_state {
            let walk_output = receiver_snap.recv_async().await?;
            let snap = walk_output.snap.clone();

            self.metadata_state = TreeMetadataState::Received(walk_output);

            let actions_deps = self.action_deps.take().unwrap();
            let mut action_ctx = ActionCtx {
                fs_tree: self.fs_tree.clone(),
                snap,
                receiver: actions_deps.receiver_act_req,
                sender: actions_deps.sender_act_rsp,
                read_buf_size: self.config.perf_data_buffer_size,
                ongoing_write_file: None,
            };
            actions_deps
                .task_tracker
                .spawn(async move { action_ctx.task().await })?;
        }
        Ok(())
    }

    fn get_root_entry(&self) -> anyhow::Result<MutexGuard<'_, MyDirEntry>> {
        let TreeMetadataState::Received(output) = &self.metadata_state else {
            anyhow::bail!("inconsistent state, call wait_for_tree() first");
        };
        Ok(output.snap.lock().unwrap())
    }

    fn get_fs_action_requester(&self) -> ActionReqSender {
        self.sender_act_req.clone()
    }

    fn get_fs_action_responder(&self) -> ActionRspReceiver {
        self.receiver_act_rsp.clone()
    }

    fn save_snap(&mut self, sync: bool) {
        if let TreeMetadataState::Received(output) =
            std::mem::replace(&mut self.metadata_state, TreeMetadataState::Terminated)
        {
            log::info!("tree[{}]: saving snap", self.fs_tree);
            // restore last_syncs from loaded snapshot
            let mut last_syncs = std::mem::take(&mut self.last_syncs);
            if sync {
                // and update with synced remotes
                for sync_fqn in self.sync_fqns.iter() {
                    last_syncs.insert(sync_fqn.clone(), self.ts);
                }
            }
            // save metadata
            let snap = MetadataSnap {
                ts: Some(self.ts),
                path: self.path.clone(),
                last_syncs,
                root: Some(std::mem::take(&mut output.snap.lock().unwrap())),
            };
            let synced_remotes = if sync {
                self.sync_fqns
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

    async fn terminate(&mut self) {}

    fn take_prev_sync_snap(&mut self) -> Option<MetadataSnap> {
        if let TreeMetadataState::Received(output) = &mut self.metadata_state {
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
    use std::os::unix::fs::MetadataExt as _;

    use crate::config::tests::load_ut_cfg_ctx;
    use crate::generic::file::MyHash;
    use crate::generic::fs::PathExt as _;
    use crate::generic::task_tracker::TaskTrackerMain;
    use crate::proto::TimestampExt as _;
    use crate::proto::{DirectoryData, RegularData};

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

        let config = load_ut_cfg_ctx().unwrap();
        let ts = Timestamp::now();

        // walk the directory
        let mut task_tracker_main = TaskTrackerMain::default();
        let task_tracker = task_tracker_main.tracker();
        let mut tree = TreeLocal::spawn(
            config,
            &task_tracker,
            test_dir_path.checked_as_str()?,
            ts,
            vec![],
        )?;
        drop(task_tracker);

        tree.wait_for_tree().await?;

        {
            let root_entry = tree.get_root_entry()?;
            {
                let root_content = root_entry.get_dir_content(".");
                //println!("{root_content:#?}");
                assert_eq!(root_content.len(), 6);
                let file_names: Vec<_> =
                    root_content.iter().map(|e| e.file_name.as_str()).collect();
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
                let sub_content = root_entry.get_dir_content("./sub folder 2 €");
                //println!("{sub_content:#?}");
                assert_eq!(sub_content.len(), 1);
                assert_eq!(sub_content[0].file_name, "beta");
            }
            {
                let entry = root_entry.get_entry("./sub folder 2 €/beta").unwrap();
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
                let entry = root_entry.get_entry("./some_link").unwrap();
                let Some(Specific::Symlink(symlink_data)) = &entry.specific else {
                    panic!("some_link error");
                };
                assert_eq!(symlink_data, "target");
            }
        }

        task_tracker_main.request_stop();
        task_tracker_main.wait().await?;
        Ok(())
    }

    struct TestSetup {
        temp_dir: tempfile::TempDir,
        fs_tree: Arc<FsTree>,
        sender_req: Sender<Arc<ActionReq>>,
        receiver_rsp: Receiver<ActionRsp>,
        snap: Arc<Mutex<MyDirEntry>>,
        my_uid: u32,
        my_gid: u32,
    }

    impl TestSetup {
        fn new() -> anyhow::Result<Self> {
            let temp_dir = tempfile::tempdir()?;
            let temp_path = temp_dir.path().to_str().unwrap();
            let fs_tree = Arc::new(FsTree::new(temp_path)?);

            let meta = std::fs::metadata(temp_path)?;
            let my_uid = meta.uid();
            let my_gid = meta.gid();

            let (sender_req, receiver_req) = flume::bounded(10);
            let (sender_rsp, receiver_rsp) = flume::bounded(10);

            let snap = Arc::new(Mutex::new(MyDirEntry {
                file_name: ".".to_string(),
                specific: Some(Specific::Directory(DirectoryData { content: vec![] })),
                permissions: 0o755,
                uid: my_uid,
                gid: my_gid,
                mtime: Some(Timestamp::now()),
            }));

            let mut action_ctx = ActionCtx {
                fs_tree: fs_tree.clone(),
                snap: snap.clone(),
                receiver: receiver_req,
                sender: sender_rsp,
                read_buf_size: 1024,
                ongoing_write_file: None,
            };

            // Spawn the task
            tokio::spawn(async move { action_ctx.task().await });

            Ok(Self {
                temp_dir,
                fs_tree,
                sender_req,
                receiver_rsp,
                snap,
                my_uid,
                my_gid,
            })
        }

        async fn end_marker(&self) -> anyhow::Result<()> {
            self.sender_req
                .send_async(Arc::new(ActionReq::EndMarker(PROTO_NULL_VALUE)))
                .await?;
            let rsp = self.receiver_rsp.recv_async().await?;
            assert!(matches!(rsp, ActionRsp::EndMarker(_)));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_action_create_dir() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let setup = TestSetup::new()?;

        // 1. Create directory
        let dir_meta = MyDirEntry {
            file_name: "subdir".to_string(),
            specific: Some(Specific::Directory(DirectoryData { content: vec![] })),
            permissions: 0o755,
            uid: setup.my_uid,
            gid: setup.my_gid,
            mtime: Some(Timestamp {
                seconds: 1000,
                nanos: 0,
            }),
        };
        setup
            .sender_req
            .send_async(Arc::new(ActionReq::CreateUpdateMetadata(
                UpdateMetadataReq {
                    rel_path: "./subdir".to_string(),
                    metadata: Some(dir_meta.clone()),
                },
            )))
            .await?;
        setup.end_marker().await?;

        // Verify on filesystem
        let stat = setup.fs_tree.stat("./subdir")?;
        assert!(stat.is_dir());

        // Verify snap
        let snap = setup.snap.lock().unwrap();
        let entry = snap.get_entry("./subdir").unwrap();
        assert_eq!(entry.file_name, "subdir");
        assert!(entry.is_dir());

        Ok(())
    }

    #[tokio::test]
    async fn test_action_create_file() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let setup = TestSetup::new()?;

        // Create parent dir first
        setup
            .fs_tree
            .mkdir("./subdir")
            .expect("Failed to create subdir");
        setup
            .snap
            .lock()
            .unwrap()
            .create_or_update_entry(
                "./subdir",
                MyDirEntry {
                    file_name: "subdir".to_string(),
                    specific: Some(Specific::Directory(DirectoryData { content: vec![] })),
                    permissions: 0o755,
                    uid: setup.my_uid,
                    gid: setup.my_gid,
                    mtime: Some(Timestamp::now()),
                },
            )
            .unwrap();

        let file_content = b"Hello World";
        let file_hash = blake3::hash(file_content).as_bytes().to_vec();
        let file_meta = MyDirEntry {
            file_name: "file.txt".to_string(),
            specific: Some(Specific::Regular(RegularData {
                size: file_content.len() as u64,
                hash: file_hash.clone(),
            })),
            permissions: 0o644,
            uid: setup.my_uid,
            gid: setup.my_gid,
            mtime: Some(Timestamp {
                seconds: 2000,
                nanos: 0,
            }),
        };

        // Send first chunk (creation)
        setup
            .sender_req
            .send_async(Arc::new(ActionReq::CreateUpdateFile(FileWriteReq {
                rel_path: "./subdir/file.txt".to_string(),
                offset: 0,
                data: file_content[..5].to_vec(),
                size: Some(file_content.len() as u64),
                metadata: None,
            })))
            .await?;

        // Send second chunk (append + finalize)
        setup
            .sender_req
            .send_async(Arc::new(ActionReq::CreateUpdateFile(FileWriteReq {
                rel_path: "./subdir/file.txt".to_string(),
                offset: 5,
                data: file_content[5..].to_vec(),
                size: None,
                metadata: Some(file_meta.clone()),
            })))
            .await?;
        setup.end_marker().await?;

        // Verify on filesystem
        let stat = setup.fs_tree.stat("./subdir/file.txt")?;
        assert!(stat.is_file());
        let f = setup.fs_tree.open("./subdir/file.txt")?;
        assert_eq!(f.file_size()?, file_content.len() as u64);

        // Verify snap
        let snap = setup.snap.lock().unwrap();
        let entry = snap.get_entry("./subdir/file.txt").unwrap();
        assert_eq!(entry.file_name, "file.txt");
        assert!(entry.is_file());
        if let Some(Specific::Regular(data)) = &entry.specific {
            assert_eq!(data.size, file_content.len() as u64);
            assert_eq!(data.hash, file_hash);
        } else {
            panic!("Expected regular file");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_action_read_file() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let setup = TestSetup::new()?;
        let file_content = b"read me";
        std::fs::write(setup.temp_dir.path().join("file.txt"), file_content)?;

        setup
            .sender_req
            .send_async(Arc::new(ActionReq::ReadFile(FileReadReq {
                rel_path: "./file.txt".to_string(),
            })))
            .await?;

        let rsp = setup.receiver_rsp.recv_async().await?;
        if let ActionRsp::FileData(read_rsp) = rsp {
            assert_eq!(read_rsp.rel_path, "./file.txt");
            assert_eq!(read_rsp.offset, 0);
            assert_eq!(read_rsp.data, file_content);
        } else {
            panic!("Expected FileData response, got {rsp:?}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_action_update_metadata() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let setup = TestSetup::new()?;
        let file_path = setup.temp_dir.path().join("file.txt");
        std::fs::write(&file_path, "content")?;
        let file_meta = setup.fs_tree.stat("./file.txt")?;
        setup
            .snap
            .lock()
            .unwrap()
            .create_or_update_entry("./file.txt", file_meta)
            .unwrap();

        let mut new_file_meta = setup.fs_tree.stat("./file.txt")?;
        new_file_meta.permissions = 0o600;
        setup
            .sender_req
            .send_async(Arc::new(ActionReq::CreateUpdateMetadata(
                UpdateMetadataReq {
                    rel_path: "./file.txt".to_string(),
                    metadata: Some(new_file_meta),
                },
            )))
            .await?;
        setup.end_marker().await?;

        // Verify on filesystem
        let stat = setup.fs_tree.stat("./file.txt")?;
        assert_eq!(stat.permissions & 0o777, 0o600);

        // Verify snap
        let snap = setup.snap.lock().unwrap();
        let entry = snap.get_entry("./file.txt").unwrap();
        assert_eq!(entry.permissions & 0o777, 0o600);

        Ok(())
    }

    #[tokio::test]
    async fn test_action_delete_file() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let setup = TestSetup::new()?;
        let file_path = setup.temp_dir.path().join("file.txt");
        std::fs::write(&file_path, "content")?;
        let file_meta = setup.fs_tree.stat("./file.txt")?;
        setup
            .snap
            .lock()
            .unwrap()
            .create_or_update_entry("./file.txt", file_meta)
            .unwrap();

        setup
            .sender_req
            .send_async(Arc::new(ActionReq::DeleteFile(DeleteEntryReq {
                rel_path: "./file.txt".to_string(),
            })))
            .await?;
        setup.end_marker().await?;

        // Verify on filesystem
        assert!(!file_path.exists());

        // Verify snap
        let snap = setup.snap.lock().unwrap();
        assert!(snap.get_entry("./file.txt").is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_action_delete_dir() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let setup = TestSetup::new()?;
        let dir_path = setup.temp_dir.path().join("subdir");
        std::fs::create_dir(&dir_path)?;
        let dir_meta = setup.fs_tree.stat("./subdir")?;
        setup
            .snap
            .lock()
            .unwrap()
            .create_or_update_entry("./subdir", dir_meta)
            .unwrap();

        setup
            .sender_req
            .send_async(Arc::new(ActionReq::DeleteDir(DeleteEntryReq {
                rel_path: "./subdir".to_string(),
            })))
            .await?;
        setup.end_marker().await?;

        // Verify on filesystem
        assert!(!dir_path.exists());

        // Verify snap
        let snap = setup.snap.lock().unwrap();
        assert!(snap.get_entry("./subdir").is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_action_create_symlink() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let setup = TestSetup::new()?;

        let link_meta = MyDirEntry {
            file_name: "mylink".to_string(),
            specific: Some(Specific::Symlink("target".to_string())),
            permissions: 0o777,
            uid: setup.my_uid,
            gid: setup.my_gid,
            mtime: Some(Timestamp {
                seconds: 3000,
                nanos: 0,
            }),
        };
        setup
            .sender_req
            .send_async(Arc::new(ActionReq::CreateUpdateMetadata(
                UpdateMetadataReq {
                    rel_path: "./mylink".to_string(),
                    metadata: Some(link_meta.clone()),
                },
            )))
            .await?;
        setup.end_marker().await?;

        // Verify on filesystem
        let stat = setup.fs_tree.stat("./mylink")?;
        if let Some(Specific::Symlink(target)) = stat.specific {
            assert_eq!(target, "target");
        } else {
            panic!("Expected symlink");
        }

        // Verify snap
        let snap = setup.snap.lock().unwrap();
        let entry = snap.get_entry("./mylink").unwrap();
        assert!(entry.is_symlink());
        if let Some(Specific::Symlink(target)) = &entry.specific {
            assert_eq!(target, "target");
        } else {
            panic!("Expected symlink in snap");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_action_error_handling() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let setup = TestSetup::new()?;

        setup
            .sender_req
            .send_async(Arc::new(ActionReq::ReadFile(FileReadReq {
                rel_path: "./non_existent".to_string(),
            })))
            .await?;

        let rsp = setup.receiver_rsp.recv_async().await?;
        if let ActionRsp::Error(err) = rsp {
            assert_eq!(err.rel_path, "./non_existent");
        } else {
            panic!("Expected Error response, got {rsp:?}");
        }

        Ok(())
    }
}
