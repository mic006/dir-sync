//! dir-sync entry point

mod dir_stat;
mod dir_walk;
pub mod generic {
    pub mod task_tracker;
    pub mod test;
}
mod hasher;
pub mod proto;

fn main() -> anyhow::Result<std::process::ExitCode> {
    dir_sync_intg()
}

#[allow(clippy::unnecessary_wraps)]
fn dir_sync_intg() -> anyhow::Result<std::process::ExitCode> {
    Ok(std::process::ExitCode::SUCCESS)
}
