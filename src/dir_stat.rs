//! Complete a `MetadataSnap` by adding files hash

use std::path::{Path, PathBuf};

use crate::dir_walk::DirWalkReceiver;
use crate::generic::fs::MessageExt;
use crate::generic::task_tracker::{TaskExit, TaskTracker};
use crate::proto::{MetadataSnap, MetadataSnapExt as _, MyDirEntryExt as _, Specific};

/// Hash file, using blake3
fn hash(path: &Path) -> anyhow::Result<blake3::Hash> {
    let mut hasher = blake3::Hasher::new();
    hasher.update_mmap_rayon(path)?;
    Ok(hasher.finalize())
}

pub struct DirStat {
    path: PathBuf,
    receiver: DirWalkReceiver,
    snap: MetadataSnap,
}
impl DirStat {
    pub fn spawn(
        task_tracker: &TaskTracker,
        path: PathBuf,
        receiver: DirWalkReceiver,
    ) -> anyhow::Result<()> {
        let snap = MetadataSnap::new(&path)?;
        let instance = Self {
            path,
            receiver,
            snap,
        };
        task_tracker.spawn_blocking(|| instance.task())?;
        Ok(())
    }

    fn task(mut self) -> anyhow::Result<TaskExit> {
        while let Ok(mut input) = self.receiver.recv() {
            let rel_path = self.path.join(&input.rel_path);
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
            self.snap
                .root
                .as_mut()
                .unwrap()
                .insert(&input.rel_path, input.entries)?;
        }

        // TODO: remove
        self.snap
            .save_to_file(Path::new("/tmp/autoclean/meta_snap.pb.bin.zst"))?;

        Ok(TaskExit::SecondaryTaskKeepRunning)
    }
}
