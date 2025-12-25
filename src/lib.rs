//! Library

use std::path::Path;

mod dir_stat;

/// Dummy main function
///
/// # Errors
/// N/A
pub async fn lib_main() -> anyhow::Result<std::process::ExitCode> {
    let mut args = std::env::args();
    let _skip_program_name = args.next();
    let path = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("Usage: dir-sync <path>"))?;

    let dir_content = dir_stat::DirStat::new(Path::new(&path)).task()?;

    for f in dir_content {
        println!("{}", f.display());
    }
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    Ok(std::process::ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    #[test]
    fn dummy_test() {}
}
