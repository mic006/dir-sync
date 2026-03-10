//! Generic interface for one input of `dir-sync`

use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use flume::{Receiver, Sender};
use regex::Regex;

use crate::proto::{ActionReq, ActionRsp, MetadataSnap, MyDirEntry};

pub type ActionReqSender = Sender<Arc<ActionReq>>;
pub type ActionRspReceiver = Receiver<ActionRsp>;

/// Generic access to one Tree (local or remote)
#[async_trait::async_trait]
pub trait Tree {
    /// Wait for tree to be available
    /// Shall be cancellable safe
    async fn wait_for_tree(&mut self) -> anyhow::Result<()>;

    /// Get reference to root entry of the tree
    ///
    /// # Errors
    /// * no root entry available (need to call `wait_for_tree()` first)
    fn get_root_entry(&self) -> anyhow::Result<MutexGuard<'_, MyDirEntry>>;

    /// Get sender to send FS action requests
    fn get_fs_action_requester(&self) -> ActionReqSender;

    /// Get receiver to get FS action responses
    fn get_fs_action_responder(&self) -> ActionRspReceiver;

    /// Save snap file
    ///
    /// Save snapshot file, update sync information if `sync` is true
    fn save_snap(&mut self, sync: bool);

    /// Clean termination of the tree
    async fn terminate(&mut self);

    /// Get snapshot of the previous synchronization
    fn take_prev_sync_snap(&mut self) -> Option<MetadataSnap>;
}

/// Path to one tree (local or remote)
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
            let hostname = &captures[1];
            let path = &captures[2];
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

/// State for metadata
pub enum TreeMetadataState {
    /// Walking of the directory is on-going, handler to get the result
    Processing(Receiver<TreeWalkOutput>),
    /// Result is already available
    Received(TreeWalkOutput),
    /// Result has been consumed
    Terminated,
}

/// Output of the walk task
pub struct TreeWalkOutput {
    /// Metadata snapshot of the tree
    pub snap: Arc<Mutex<MyDirEntry>>,
    /// Previous sync snapshot for the tree, if any
    pub prev_sync_snap: Option<MetadataSnap>,
}
