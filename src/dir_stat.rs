//! Complete a `MetadataSnap` by adding files hash

use std::path::{Path, PathBuf};

use crate::config::ConfigRef;
use crate::dir_walk::DirWalkReceiver;
use crate::generic::fs::MessageExt;
use crate::generic::task_tracker::{TaskExit, TaskTracker};
use crate::proto::{
    MetadataSnap, MetadataSnapExt as _, MyDirEntryExt as _, Specific, get_metadata_snap_path,
};

/// Hash file, using blake3
fn hash(path: &Path) -> anyhow::Result<blake3::Hash> {
    let mut hasher = blake3::Hasher::new();
    hasher.update_mmap_rayon(path)?;
    Ok(hasher.finalize())
}

pub struct DirStat {
    config: ConfigRef,
    base: PathBuf,
    receiver: DirWalkReceiver,
    prev_snap: Option<MetadataSnap>,
    snap: MetadataSnap,
}
impl DirStat {
    pub fn spawn(
        config: ConfigRef,
        task_tracker: &TaskTracker,
        path: &Path,
        receiver: DirWalkReceiver,
    ) -> anyhow::Result<()> {
        let snap = MetadataSnap::new(path)?;
        let prev_snap = MetadataSnap::load_from_file(get_metadata_snap_path(&config, path)).ok();
        let instance = Self {
            config,
            base: path.into(),
            receiver,
            prev_snap,
            snap,
        };
        task_tracker.spawn(async move { instance.task().await })?;
        Ok(())
    }

    async fn task(mut self) -> anyhow::Result<TaskExit> {
        while let Ok(mut input) = self.receiver.recv_async().await {
            log::debug!(
                "stat[{}]: entering {}",
                self.base.display(),
                input.rel_path.display()
            );
            let rel_path = self.base.join(&input.rel_path);
            // add hash of files
            for f in &mut input.entries {
                if let Some(Specific::Regular(file_data)) = &mut f.specific
                    && file_data.size != 0
                {
                    let path = rel_path.join(&f.file_name);
                    file_data.hash = hash(&path)?.as_bytes().into();
                }
            }

            // add in collection
            self.snap.insert(&input.rel_path, input.entries)?;
        }

        // TODO: remove
        self.snap
            .save_to_file(get_metadata_snap_path(&self.config, &self.base))?;

        log::debug!("stat[{}]: completed", self.base.display());
        // TODO: Ok(TaskExit::SecondaryTaskKeepRunning)
        Ok(TaskExit::MainTaskStopAppSuccess)
    }
}
