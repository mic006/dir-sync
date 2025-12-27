use std::process::Command;

/// Custom build script
/// - generate `BUILD_GIT_VERSION` from `git describe --tags --always --dirty`
/// - generate rust code from `src/proto/*.proto` files using Prost
fn main() -> anyhow::Result<()> {
    // == BUILD_GIT_VERSION generation ==
    // run when HEAD changes
    println!("cargo::rerun-if-changed=.git/HEAD");
    // run when new commit is done on the working branch
    println!("cargo::rerun-if-changed=.git/refs/heads");
    // run when new tag is added
    println!("cargo::rerun-if-changed=.git/refs/tags");
    // run if any change in the directory
    println!("cargo::rerun-if-changed=.");

    if let Some(version) = git_version() {
        println!("cargo::rustc-env=BUILD_GIT_VERSION={version}");
    }

    // == Prost generation from .proto files ==
    // get list of proto files
    let protos = std::fs::read_dir("src/proto")?
        .map(|res| res.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()?;

    // indicate dependency to cargo
    for filename in &protos {
        println!(
            "cargo:rerun-if-changed=src/proto/{}",
            filename.to_str().expect("invalid filename")
        );
    }

    // generate rust component from protos
    prost_build::Config::new()
        .include_file("mod.rs")
        .compile_protos(&protos, &["src/proto"])?;

    Ok(())
}

/// Get git project version via `git describe --tags --always --dirty`
fn git_version() -> Option<String> {
    let output = Command::new("git")
        .args(["describe", "--tags", "--always", "--dirty"])
        .output()
        .ok()?;
    output.status.success().then_some(())?;
    String::from_utf8_lossy(&output.stdout)
        .strip_suffix('\n')
        .map(ToOwned::to_owned)
}
