//! Generic interface for one input of `dir-sync`

use std::path::Path;

use crate::proto::MyDirEntry;

/// Tree access
pub trait DirTree {
    /// Get one entry in the tree
    fn get_entry(&self, rel_path: &Path) -> Option<&MyDirEntry>;

    /// Get content of one directory in the tree
    fn get_dir_content(&self, rel_path: &Path) -> &[MyDirEntry];
}

//#[async_trait::async_trait]
pub trait DirAction {
    async fn delete_entry(&mut self, rel_path: &Path) -> anyhow::Result<()>;

    async fn 
}
