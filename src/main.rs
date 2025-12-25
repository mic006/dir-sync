#[tokio::main]
async fn main() -> anyhow::Result<std::process::ExitCode> {
    dir_sync::lib_main().await
}
