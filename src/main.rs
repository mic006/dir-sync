//! dir-sync entry point

use std::path::PathBuf;

use clap::Parser as _;

pub mod config;
mod dir_stat;
mod dir_walk;
pub mod generic {
    pub mod task_tracker;
    pub mod test;
}
mod hasher;
pub mod proto;

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

fn main() -> anyhow::Result<std::process::ExitCode> {
    let arg = Arg::parse();
    println!("arg={arg:?}");
    dir_sync_intg()
}

#[allow(clippy::unnecessary_wraps)]
fn dir_sync_intg() -> anyhow::Result<std::process::ExitCode> {
    Ok(std::process::ExitCode::SUCCESS)
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
