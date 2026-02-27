//! dir-sync entry point

use rayon::prelude::*;

use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser as _;
use prost_types::Timestamp;
use regex::Regex;

use crate::config::{Config, FileMatcher};
use crate::diff::DiffMode;
use crate::generic::fs::{MessageExt as _, PathExt as _};
use crate::generic::libc::reset_sigpipe;
use crate::generic::task_tracker::{TaskExit, TaskTracker, TaskTrackerMain, TrackedTaskResult};
use crate::proto::{MetadataSnap, MyDirEntry, TimestampExt as _};
use crate::snap::list_snaps_stdout;
use crate::sync_plan::SyncMode;
use crate::tree::Tree;
use crate::tree_local::TreeLocal;

pub mod config;
pub mod diff;
pub mod generic {
    pub mod config {
        pub mod field_mem_size;
    }
    pub mod file;
    pub mod format {
        pub mod hash;
        pub mod owner;
        pub mod permissions;
        pub mod size;
        pub mod timestamp;
        pub mod tree;
    }
    pub mod fs;
    pub mod iter;
    pub mod libc;
    pub mod path_regex;
    pub mod prost_stream;
    pub mod task_tracker;
    pub mod test;
}
pub mod output;
pub mod proto;
pub mod remote;
pub mod snap;
pub mod sync_exec;
pub mod sync_plan;
pub mod tree;
pub mod tree_local;

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
    #[arg(short('L'), long)]
    log: Option<PathBuf>,
    /// Enable debug output
    #[arg(short, long)]
    debug: bool,
    /// Resolve all unresolved conflicts by taking the latest file by modification time
    /// CAUTION: use this option with care
    /// - conflicting changes will be overridden
    /// - deleted or moved files will be restored
    #[clap(verbatim_doc_comment)]
    #[arg(long)]
    latest: bool,
    /// Dry run: sync operations will be displayed but not executed
    #[arg(short('n'), long)]
    dry_run: bool,
    /// Directories to compare
    #[arg()]
    dirs: Vec<String>,
}

/// Mode of operation
#[derive(clap::Args, Debug)]
#[group(multiple = false)]
#[allow(clippy::struct_excessive_bools)]
struct Mode {
    /// Stop on first difference and exit with failure status (script)
    #[arg(short('s'), long, help_heading = "Mode")]
    status: bool,
    /// Output the differences to stdout, one diff per line
    #[arg(short('o'), long, help_heading = "Mode")]
    output: bool,
    /// Perform automatic synchronization, ignoring conflicts
    #[arg(short('b'), long, help_heading = "Mode")]
    sync_batch: bool,
    /// List user's metadata snapshots to stdout
    #[arg(short('l'), long, help_heading = "Mode")]
    list_metadata_snap: bool,
    /// Refresh the metadata snapshot of a single source
    #[arg(short('R'), long, help_heading = "Mode", hide_short_help = true)]
    refresh_metadata_snap: bool,
    /// Dump the metadata snapshot content to stdout
    #[arg(short('P'), long, help_heading = "Mode", hide_short_help = true)]
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
            } else if mode.list_metadata_snap {
                RunMode::ListMetadataSnap
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
    /// List user's metadata snapshots to stdout
    ListMetadataSnap,
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
    match run_mode {
        RunMode::ListMetadataSnap => {
            anyhow::ensure!(
                arg.dirs.is_empty(),
                "ListMetadataSnap mode: expects no extra argument"
            );
            let config = Arc::new(Config::from_file(None)?);
            list_snaps_stdout(&config);
            Ok(std::process::ExitCode::SUCCESS)
        }
        RunMode::DumpMetadataSnap => {
            anyhow::ensure!(
                arg.dirs.len() == 1,
                "DumpMetadataSnap mode: expects a single file"
            );
            let snap_file = &arg.dirs[0];
            let snap = MetadataSnap::load_from_file(snap_file)?;
            output::output(&snap);
            Ok(std::process::ExitCode::SUCCESS)
        }
        _ => {
            // async modes
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async move { async_main(arg, run_mode).await })
        }
    }
}

/// Async main function
async fn async_main(arg: Arg, run_mode: RunMode) -> anyhow::Result<std::process::ExitCode> {
    let task_tracker_main = TaskTrackerMain::default();
    task_tracker_main.setup_signal_catching()?;

    if let Some(log_file) = &arg.log {
        let mut logger = env_logger::Builder::from_env(
            env_logger::Env::default().default_filter_or(if arg.debug { "debug" } else { "info" }),
        );
        logger
            .format_source_path(true)
            .format_target(false)
            .format_timestamp_millis();
        if log_file != Path::new("stderr") {
            let target = Box::new(File::create(log_file).expect("Can't create log file"));
            logger.target(env_logger::Target::Pipe(target));
        }
        logger.init();
    }

    match run_mode {
        RunMode::TerminalUI => todo!(),
        RunMode::Status => {
            let task_tracker = task_tracker_main.tracker();
            task_tracker_main
                .spawn(async move { diff_main(task_tracker, arg, DiffMode::Status).await })?;
        }
        RunMode::Output => {
            let task_tracker = task_tracker_main.tracker();
            task_tracker_main
                .spawn(async move { diff_main(task_tracker, arg, DiffMode::Output).await })?;
        }
        RunMode::SyncBatch => {
            let task_tracker = task_tracker_main.tracker();
            task_tracker_main.spawn(async move { sync_main(task_tracker, arg).await })?;
        }
        RunMode::RefreshMetadataSnap => {
            let task_tracker = task_tracker_main.tracker();
            task_tracker_main
                .spawn(async move { refresh_metadata_snap(task_tracker, arg).await })?;
        }
        RunMode::Remote => {
            let task_tracker = task_tracker_main.tracker();
            task_tracker_main.spawn(async move { remote::remote_main(task_tracker).await })?;
        }
        RunMode::ListMetadataSnap | RunMode::DumpMetadataSnap => unreachable!("handled in main()"),
    }

    // run until completion or error
    task_tracker_main.wait().await
}

async fn diff_main(task_tracker: TaskTracker, arg: Arg, mode: DiffMode) -> TrackedTaskResult {
    anyhow::ensure!(
        arg.dirs.len() >= 2,
        "Diff mode: expects at least 2 directories"
    );

    // spawn all trees
    let mut ctx = RunContext::new(&task_tracker, &arg, false)?;
    // wait for tree walk completion
    for tree in &mut ctx.trees {
        tree.wait_for_tree().await?;
    }

    // perform diff
    let diffs = crate::diff::diff_trees(task_tracker, &ctx.trees, mode);
    ctx.save_snaps(false);
    let diffs = diffs?;

    match mode {
        DiffMode::Status => {
            if diffs.is_empty() {
                Ok(TaskExit::MainTaskStopAppSuccess)
            } else {
                Ok(TaskExit::MainTaskStopAppFailure(ExitCode::FAILURE))
            }
        }
        DiffMode::Output => {
            for diff in diffs {
                if writeln!(std::io::stdout(), "{diff}").is_err() {
                    // stdout has been closed, stop
                    break;
                }
            }
            Ok(TaskExit::MainTaskStopAppSuccess)
        }
    }
}

async fn sync_main(task_tracker: TaskTracker, arg: Arg) -> TrackedTaskResult {
    anyhow::ensure!(
        arg.dirs.len() >= 2,
        "Sync mode: expects at least 2 directories"
    );

    // spawn all trees
    let mut ctx = RunContext::new(&task_tracker, &arg, true)?;
    // wait for tree walk completion
    for tree in &mut ctx.trees {
        tree.wait_for_tree().await?;
    }

    // perform diff
    let mut diffs = crate::diff::diff_trees(task_tracker.clone(), &ctx.trees, DiffMode::Output)?;

    // get prev_snaps from all trees
    let prev_sync_snaps = ctx.get_prev_sync_snaps();

    // determine sync plan
    let sync_mode = if arg.latest {
        SyncMode::Latest
    } else {
        SyncMode::Standard
    };
    crate::sync_plan::sync_plan(task_tracker.clone(), prev_sync_snaps, sync_mode, &mut diffs)?;

    if arg.dry_run {
        ctx.save_snaps(false);
        let mut stdout = std::io::stdout();
        if diffs.is_empty() {
            let _ignored = writeln!(stdout, "No difference found between inputs");
        } else {
            // print differences with sync source
            for diff in diffs {
                if writeln!(stdout, "{diff:#}").is_err() {
                    // stdout has been closed, stop
                    break;
                }
            }
        }
    } else {
        sync_exec::sync_exec(task_tracker, &mut ctx.trees, &diffs).await?;
        ctx.save_snaps(true);
    }

    Ok(TaskExit::MainTaskStopAppSuccess)
}

async fn refresh_metadata_snap(task_tracker: TaskTracker, arg: Arg) -> TrackedTaskResult {
    anyhow::ensure!(
        arg.dirs.len() == 1,
        "RefreshMetadataSnap mode: expects a single directory"
    );

    let mut ctx = RunContext::new(&task_tracker, &arg, false)?;
    ctx.trees[0].wait_for_tree().await?;
    ctx.save_snaps(false);

    Ok(TaskExit::MainTaskStopAppSuccess)
}

/// Runtime context
struct RunContext {
    /// Configuration
    config: Arc<Config>,
    /// File Matcher for selected profile
    file_matcher: Option<FileMatcher>,
    /// Timestamp of the snapshot
    ts: Timestamp,
    /// Trees to compare
    trees: Vec<Box<dyn Tree + Send + Sync>>,
}

impl RunContext {
    fn new(task_tracker: &TaskTracker, arg: &Arg, sync_mode: bool) -> anyhow::Result<Self> {
        let config = Arc::new(Config::from_file(None)?);
        let file_matcher = config.get_file_matcher(arg.profile.as_deref())?;
        let ts = Timestamp::now();

        let mut instance = Self {
            config,
            file_matcher,
            ts,
            trees: Vec::with_capacity(arg.dirs.len()),
        };
        for (index, dir) in arg.dirs.iter().enumerate() {
            let sync_paths = if sync_mode {
                arg.dirs
                    .iter()
                    .enumerate()
                    .filter_map(|(i, d)| if i == index { None } else { Some(d.clone()) })
                    .collect()
            } else {
                vec![]
            };
            instance.spawn_tree(task_tracker, dir, sync_paths)?;
        }
        Ok(instance)
    }

    fn spawn_tree(
        &mut self,
        task_tracker: &TaskTracker,
        dir: &str,
        sync_paths: Vec<String>,
    ) -> anyhow::Result<()> {
        use std::sync::LazyLock;
        static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\w+):(.+)$").unwrap());
        if let Some(_captures) = RE.captures(dir) {
            // remote tree
            todo!();
        } else {
            // local tree
            let dir = canonicalize(dir)?;
            let tree = Box::new(TreeLocal::spawn(
                self.config.clone(),
                task_tracker,
                &dir,
                self.ts,
                self.file_matcher.clone(),
                sync_paths,
            )?);
            self.trees.push(tree);
        }
        Ok(())
    }

    fn get_prev_sync_snaps(&mut self) -> Option<Vec<MyDirEntry>> {
        let prev_sync_snaps = self
            .trees
            .iter_mut()
            .map(|t| t.take_prev_sync_snap())
            .collect::<Option<Vec<_>>>()?;

        // ensure all snaps refer to the same timestamp
        let mut it = prev_sync_snaps.iter();
        let first = it.next()?;
        if it.any(|snap| snap.ts != first.ts) {
            return None;
        }

        Some(
            prev_sync_snaps
                .into_iter()
                .map(|snap| snap.root.unwrap())
                .collect(),
        )
    }

    fn save_snaps(&mut self, sync: bool) {
        // parallelized: save snap of each tree
        self.trees.par_iter_mut().for_each(|t| t.save_snap(sync));
    }
}

fn canonicalize(p: &str) -> anyhow::Result<String> {
    let p = std::fs::canonicalize(p)?;
    Ok(p.checked_as_str()?.to_owned())
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
