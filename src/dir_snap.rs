//! Determine the `MetadataSnap` of one path

use std::path::Path;

use crate::config::{ConfigRef, FileMatcher};
use crate::dir_stat::DirStat;
use crate::dir_walk::DirWalk;
use crate::generic::task_tracker::TaskTracker;
use crate::proto::MetadataSnap;

pub type SnapOutput = (usize, MetadataSnap);

pub type SenderSnapOutput = flume::Sender<SnapOutput>;

/// Spawn
pub fn spawn_dir_snap(
    task_tracker: &TaskTracker,
    config: ConfigRef,
    file_matcher: Option<FileMatcher>,
    path: &Path,
    sender: SenderSnapOutput,
) -> anyhow::Result<()> {
    let dir_receiver = DirWalk::spawn(task_tracker, path, file_matcher)?;
    DirStat::spawn(config, task_tracker, path, dir_receiver, sender)?;
    Ok(())
}
