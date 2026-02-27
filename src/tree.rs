//! Generic interface for one input of `dir-sync`

use std::sync::{Arc, LazyLock};

use flume::{Receiver, Sender};
use regex::Regex;

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

pub struct TreePath {
    /// Local path to the tree
    pub path: String,
    /// Hostname of machine, empty if local
    pub hostname: String,
    /// Fully qualified name = "hostname:path"
    /// This is the name used to identify synchronization trees
    pub fqn: String,
}

impl TreePath {
    pub fn new(arg_path: &str) -> Self {
        static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\w+):(.+)$").unwrap());

        if let Some(captures) = RE.captures(arg_path) {
            let hostname = &captures[0];
            let path = &captures[1];
            let fqn = format!("{hostname}:{path}");
            Self {
                path: path.to_string(),
                hostname: hostname.to_string(),
                fqn,
            }
        } else {
            let hostname = std::env::var("HOSTNAME").unwrap_or("localhost".to_owned());
            let fqn = format!("{hostname}:{arg_path}");
            Self {
                path: arg_path.to_string(),
                hostname: String::new(),
                fqn,
            }
        }
    }
}
