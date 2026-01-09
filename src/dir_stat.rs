//! Complete a `MetadataSnap` by adding files hash

use std::path::{Path, PathBuf};

use crate::config::ConfigRef;
use crate::dir_walk::{DirContent, DirWalkReceiver};
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
        while let Ok(input) = self.receiver.recv_async().await {
            self.handle_input(input)?;
        }

        // TODO: remove
        self.snap
            .save_to_file(get_metadata_snap_path(&self.config, &self.base))?;

        log::debug!("stat[{}]: completed", self.base.display());
        // TODO: Ok(TaskExit::SecondaryTaskKeepRunning)
        Ok(TaskExit::MainTaskStopAppSuccess)
    }

    /// Handle one input from `DirWalk`
    fn handle_input(&mut self, mut input: DirContent) -> anyhow::Result<()> {
        log::debug!(
            "stat[{}]: entering {}",
            self.base.display(),
            input.rel_path.display()
        );
        let dir_path = self.base.join(&input.rel_path);
        let mut prev_snap_dir = self
            .prev_snap
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
                    .and_then(|s| s.get_entry_mut(Path::new(&f.file_name)));
                let prev_file_hash = prev_entry.and_then(|prev_entry| {
                    if f.mtime == prev_entry.mtime
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
                    log::debug!(
                        "stat[{}]: reusing hash of {}",
                        self.base.display(),
                        input.rel_path.join(&f.file_name).display()
                    );
                    file_data.hash = std::mem::take(prev_file_hash);
                } else {
                    // compute hash
                    log::debug!(
                        "stat[{}]: compute hash of {}",
                        self.base.display(),
                        input.rel_path.join(&f.file_name).display()
                    );
                    let path = dir_path.join(&f.file_name);
                    file_data.hash = hash(&path)?.as_bytes().into();
                }
            }
        }

        // add in collection
        self.snap.insert(&input.rel_path, input.entries)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use blake3::Hash;
    use prost_types::Timestamp;

    use crate::{
        config::tests::load_ut_cfg,
        proto::{MyDirEntry, RegularData},
    };

    use super::*;

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_dirstat_handle_input() -> anyhow::Result<()> {
        crate::generic::test::log_init();

        let test_dir = tempfile::tempdir()?;
        let test_dir_path = test_dir.path();

        std::fs::write(test_dir_path.join("new_file"), "new")?;
        std::fs::write(test_dir_path.join("modified_file"), "modified")?;

        let (_sender, receiver) = flume::unbounded();
        let config = Arc::new(load_ut_cfg().unwrap());
        let mut prev_snap = MetadataSnap::new(test_dir_path)?;
        prev_snap.insert(
            Path::new(""),
            vec![
                MyDirEntry {
                    file_name: "deleted_file".into(),
                    permissions: 0,
                    uid: 0,
                    gid: 0,
                    mtime: Some(Timestamp {
                        seconds: 123,
                        nanos: 456,
                    }),
                    specific: Some(Specific::Regular(RegularData {
                        size: 2,
                        hash: vec![],
                    })),
                },
                MyDirEntry {
                    file_name: "modified_file".into(),
                    permissions: 0,
                    uid: 0,
                    gid: 0,
                    mtime: Some(Timestamp {
                        seconds: 123,
                        nanos: 457,
                    }),
                    specific: Some(Specific::Regular(RegularData {
                        size: 8,
                        hash: vec![43],
                    })),
                },
                MyDirEntry {
                    file_name: "not_modified_file".into(),
                    permissions: 0,
                    uid: 0,
                    gid: 0,
                    mtime: Some(Timestamp {
                        seconds: 123,
                        nanos: 456,
                    }),
                    specific: Some(Specific::Regular(RegularData {
                        size: 1024,
                        hash: vec![42],
                    })),
                },
            ],
        )?;
        let mut instance = DirStat {
            config,
            base: test_dir_path.into(),
            receiver,
            prev_snap: Some(prev_snap),
            snap: MetadataSnap::new(test_dir_path)?,
        };

        let dir_content = DirContent {
            rel_path: PathBuf::new(),
            entries: vec![
                MyDirEntry {
                    file_name: "new_file".into(),
                    permissions: 0,
                    uid: 0,
                    gid: 0,
                    mtime: Some(Timestamp {
                        seconds: 127,
                        nanos: 789,
                    }),
                    specific: Some(Specific::Regular(RegularData {
                        size: 3,
                        hash: vec![],
                    })),
                },
                MyDirEntry {
                    file_name: "not_modified_file".into(),
                    permissions: 0,
                    uid: 0,
                    gid: 0,
                    mtime: Some(Timestamp {
                        seconds: 123,
                        nanos: 456,
                    }),
                    specific: Some(Specific::Regular(RegularData {
                        size: 1024,
                        hash: vec![42],
                    })),
                },
                MyDirEntry {
                    file_name: "modified_file".into(),
                    permissions: 0,
                    uid: 0,
                    gid: 0,
                    mtime: Some(Timestamp {
                        seconds: 124,
                        nanos: 458,
                    }),
                    specific: Some(Specific::Regular(RegularData {
                        size: 8,
                        hash: vec![],
                    })),
                },
            ],
        };

        instance.handle_input(dir_content)?;

        assert!(instance.snap.get_entry(Path::new("deleted_file")).is_none());

        {
            let entry = instance.snap.get_entry(Path::new("new_file")).unwrap();
            let Some(Specific::Regular(file_data)) = &entry.specific else {
                panic!("new_file error");
            };
            assert_eq!(
                Hash::from_slice(&file_data.hash)?,
                Hash::from_hex("b20ab0a020a48d349e0c64d109c441f87c9bc43d49fc701c4a5f6f1b16aa4e32")?
            );
        }

        {
            let entry = instance
                .snap
                .get_entry(Path::new("not_modified_file"))
                .unwrap();
            let Some(Specific::Regular(file_data)) = &entry.specific else {
                panic!("not_modified_file error");
            };
            assert_eq!(file_data.hash, vec![42]);
        }

        {
            let entry = instance.snap.get_entry(Path::new("modified_file")).unwrap();
            let Some(Specific::Regular(file_data)) = &entry.specific else {
                panic!("modified_file error");
            };
            assert_eq!(
                Hash::from_slice(&file_data.hash)?,
                Hash::from_hex("c173e63fee599b44f68f0a5e3ae47844cf3226881b15f4fc21c5e4516a9b9314")?
            );
        }

        Ok(())
    }
}
