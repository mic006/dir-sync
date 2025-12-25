/// Dummy main function
///
/// # Errors
/// N/A
pub async fn lib_main() -> anyhow::Result<std::process::ExitCode> {
    println!("Hello World");
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    Ok(std::process::ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    #[test]
    fn dummy_test() {}
}
