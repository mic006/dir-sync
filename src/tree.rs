//! Generic interface for one input of `dir-sync`

use std::sync::Arc;

use flume::{Receiver, Sender};

use crate::proto::{ActionReq, ActionRsp, MetadataSnap, MyDirEntry};

pub type ActionReqSender = Sender<Arc<ActionReq>>;
pub type ActionRspReceiver = Receiver<ActionRsp>;

/// Access to metadata of entries in the tree
pub trait TreeMetadata {
    /// Get one entry in the tree
    fn get_entry(&self, rel_path: &str) -> Option<&MyDirEntry>;

    /// Get content of one directory in the tree (sorted)
    fn get_dir_content(&self, rel_path: &str) -> &[MyDirEntry];
}

/// Generic access to one Tree (local or remote)
#[async_trait::async_trait]
pub trait Tree: TreeMetadata {
    /// Wait for tree to be available
    /// Shall be cancellable safe
    async fn wait_for_tree(&mut self) -> anyhow::Result<()>;

    /// Get sender to send FS action requests
    fn get_fs_action_requester(&self) -> ActionReqSender;

    /// Get receiver to get FS action responses
    fn get_fs_action_responder(&self) -> ActionRspReceiver;

    /// Save snap file
    ///
    /// Save snapshot file, update sync information if `sync` is true
    fn save_snap(&mut self, sync: bool);

    /// Get snapshot of the previous synchronization
    fn take_prev_sync_snap(&mut self) -> Option<MetadataSnap>;
}

/// Consider a `Box<dyn Tree>` as a ref to `TreeMetadata`
///
/// Used to magically consider a `&Vec<Box<dyn Tree>>` as a `&[AsRef<dyn TreeMetadata>]` parameter
impl AsRef<dyn TreeMetadata> for Box<dyn Tree + Send + Sync> {
    fn as_ref(&self) -> &(dyn TreeMetadata + 'static) {
        &**self
    }
}
