//! Generic interface for one input of `dir-sync`

use std::path::Path;

use flume::{Receiver, Sender};

use crate::proto::{ActionReq, ActionRsp, MyDirEntry};

/// Access to metadata of entries in the tree
pub trait TreeMetadata {
    /// Get one entry in the tree
    fn get_entry(&self, rel_path: &Path) -> Option<&MyDirEntry>;

    /// Get content of one directory in the tree (sorted)
    fn get_dir_content(&self, rel_path: &Path) -> &[MyDirEntry];
}

/// Generic access to one Tree (local or remote)
#[async_trait::async_trait]
pub trait Tree: TreeMetadata {
    /// Wait for tree to be available
    async fn wait_for_tree(&mut self) -> anyhow::Result<()>;

    /// Get sender to send FS action requests
    fn get_fs_action_requester(&self) -> Sender<ActionReq>;

    /// Get receiver to get FS action responses
    fn get_fs_action_responder(&self) -> Receiver<ActionRsp>;
}
