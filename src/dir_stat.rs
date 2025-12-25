//! Walk trough a directory and get metadata of its content

use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

// Get metadata content of the provided directory
// pub fn dir_stat(path: &Path) -> anyhow::Result<()> {
//     tokio::spawn(DirStat::new(path).task());
//     Ok(())
// }

pub struct DirStat {
    path: PathBuf,
}
impl DirStat {
    pub fn new(path: &Path) -> Self {
        Self { path: path.into() }
    }

    pub fn task(self) -> anyhow::Result<Vec<PathBuf>> {
        let mut dir_stack = vec![self.path.clone()];
        let mut files = vec![];

        while let Some(dir) = dir_stack.pop() {
            for d in std::fs::read_dir(&dir)? {
                let d = d?;
                let metadata = d.metadata()?;
                let ft = metadata.file_type();
                if ft.is_dir() {
                    dir_stack.push(d.path());
                } else if ft.is_file() && metadata.size() > 0 {
                    files.push(d.path());
                }
            }
        }

        //tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(files)
    }
}
