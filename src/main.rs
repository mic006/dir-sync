//! dir-sync entry point

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser as _;

use crate::config::Config;
use crate::dir_stat::DirStat;
use crate::dir_walk::DirWalk;
use crate::generic::fs::MessageExt as _;
use crate::generic::libc::reset_sigpipe;
use crate::generic::task_tracker::{TaskExit, TaskTracker, TaskTrackerMain, TrackedTaskResult};
use crate::proto::MetadataSnap;

pub mod config;
mod dir_stat;
mod dir_walk;
pub mod generic {
    pub mod file;
    pub mod fs;
    pub mod libc;
    pub mod path_regex;
    pub mod task_tracker;
    pub mod test;
}
pub mod proto;
pub mod tree;

/// Dir-sync
///
/// `dir-sync` is a command line tool to view the differences and synchronize N directories.
///
/// It uses a Terminal UI interface, unless a Mode option is provided.
#[derive(clap::Parser, Debug)]
#[command(version(env!("BUILD_GIT_VERSION")))]
struct Arg {
    #[clap(flatten)]
    mode: Option<Mode>,
    /// Profile used to ignore files
    #[arg(short, long)]
    profile: Option<String>,
    /// Log target: stderr or file location
    #[arg(short, long)]
    log: Option<PathBuf>,
    /// Directories to compare
    #[arg(required = true)]
    dirs: Vec<PathBuf>,
}

/// Mode of operation
#[derive(clap::Args, Debug)]
#[group(multiple = false)]
#[allow(clippy::struct_excessive_bools)]
struct Mode {
    /// Stop on first difference and exit with failure status (script)
    #[arg(long, help_heading = "Mode")]
    status: bool,
    /// Output the differences to stdout, one diff per line
    #[arg(long, help_heading = "Mode")]
    output: bool,
    /// Perform automatic synchronization, ignoring conflicts
    #[arg(long, help_heading = "Mode")]
    sync_batch: bool,
    /// Refresh the metadata snapshot of a single source
    #[arg(long, help_heading = "Mode", hide_short_help = true)]
    refresh_metadata_snap: bool,
    /// Dump the metadata snapshot content to stdout
    #[arg(long, help_heading = "Mode", hide_short_help = true)]
    dump_metadata_snap: bool,
    /// Remote session, spawned by the master session over SSH (not for end user)
    #[arg(long, hide = true)]
    remote: bool,
}

impl Arg {
    /// Get run mode
    pub fn run_mode(&self) -> RunMode {
        if let Some(mode) = &self.mode {
            if mode.status {
                RunMode::Status
            } else if mode.output {
                RunMode::Output
            } else if mode.sync_batch {
                RunMode::SyncBatch
            } else if mode.refresh_metadata_snap {
                RunMode::RefreshMetadataSnap
            } else if mode.dump_metadata_snap {
                RunMode::DumpMetadataSnap
            } else {
                RunMode::Remote
            }
        } else {
            RunMode::TerminalUI
        }
    }
}

#[derive(PartialEq, Debug)]
enum RunMode {
    /// Terminal UI
    TerminalUI,
    /// Stop on first difference and exit with failure status (script)
    Status,
    /// Output the differences to stdout, one diff per line
    Output,
    /// Perform automatic synchronization, ignoring conflicts
    SyncBatch,
    /// Refresh the metadata snapshot of a single source
    RefreshMetadataSnap,
    /// Dump the metadata snapshot content to stdout
    DumpMetadataSnap,
    /// Remote session, spawned by the master session over SSH (not for end user)
    Remote,
}

/// Entry point
fn main() -> anyhow::Result<std::process::ExitCode> {
    reset_sigpipe();

    let arg = Arg::parse();
    let run_mode = arg.run_mode();
    if run_mode == RunMode::DumpMetadataSnap {
        anyhow::ensure!(
            arg.dirs.len() == 1,
            "DumpMetadataSnap mode: expects a single file"
        );
        let snap_file = &arg.dirs[0];
        let snap = MetadataSnap::load_from_file(snap_file)?;
        println!("{snap:#?}");
        Ok(std::process::ExitCode::SUCCESS)
    } else {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async move { async_main(arg, run_mode).await })
    }
}

/// Async main function
async fn async_main(arg: Arg, run_mode: RunMode) -> anyhow::Result<std::process::ExitCode> {
    let task_tracker_main = TaskTrackerMain::default();
    task_tracker_main.setup_signal_catching()?;

    if let Some(log_file) = &arg.log {
        let mut logger =
            env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug"));
        logger.format_source_path(true).format_target(false);
        if log_file != Path::new("stderr") {
            let target = Box::new(File::create(log_file).expect("Can't create log file"));
            logger.target(env_logger::Target::Pipe(target));
        }
        logger.init();
    }

    match run_mode {
        RunMode::RefreshMetadataSnap => {
            let task_tracker = task_tracker_main.tracker();
            task_tracker_main
                .spawn(async move { refresh_metadata_snap(task_tracker, arg).await })?;
        }
        RunMode::DumpMetadataSnap => unreachable!("handled in main()"),
        _ => todo!(),
    }

    // run until completion or error
    task_tracker_main.wait().await
}

#[allow(clippy::unused_async)]
async fn refresh_metadata_snap(task_tracker: TaskTracker, arg: Arg) -> TrackedTaskResult {
    anyhow::ensure!(
        arg.dirs.len() == 1,
        "RefreshMetadataSnap mode: expects a single directory"
    );
    let dir = std::fs::canonicalize(&arg.dirs[0])?;

    let config = Arc::new(Config::from_file(None)?);

    let dir_receiver = DirWalk::spawn(&task_tracker, &dir, None)?;
    DirStat::spawn(config, &task_tracker, &dir, dir_receiver)?;
    Ok(TaskExit::SecondaryTaskKeepRunning)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn arg_consistency() {
        Arg::command().debug_assert();
    }
}
