//! Manage one `Tree` on a remote machine
//!
//! Spawn a dir-sync process via SSH on the remote machine
//! and control this instance

use std::process::Stdio;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};

use crate::config::ConfigRef;
use crate::generic::prost_stream::{ProstRead, ProstWrite};
use crate::generic::task_tracker::{TaskExit, TaskTracker};
use crate::proto::remote::{Request, Response};
use crate::proto::{
    self, ActionReq, ActionRsp, MetadataSnap, MyDirEntry, MyDirEntryExt as _,
    REMOTE_PROTOCOL_VERSION, RemoteReq, RemoteRsp, Specific, Timestamp,
};
use crate::tree::{
    ActionReqSender, ActionRspReceiver, Tree, TreeMetadata, TreeMetadataState, TreePath,
    TreeWalkOutput,
};

pub struct TreeRemote {
    /// Instance of `dir-sync` running on remote over SSH
    child: Child,
    /// Action requester
    sender_act_req: ActionReqSender,
    /// Action responder
    receiver_act_rsp: ActionRspReceiver,
    /// Current state
    metadata_state: TreeMetadataState,
}

impl TreeRemote {
    /// Spawn the walking of a remote directory
    ///
    /// # Errors
    /// - failure to spawn dir-sync process over SSH
    /// - communication errors
    #[allow(clippy::cast_possible_truncation)]
    pub async fn spawn(
        config: ConfigRef,
        task_tracker: &TaskTracker,
        tp: &TreePath,          // path to walk
        ts: Timestamp,          // reference timestamp
        sync_fqns: Vec<String>, // other paths to perform synchronization with
    ) -> anyhow::Result<Self> {
        // 1. spawn instance
        let mut child = Command::new("ssh")
            .arg(&tp.hostname)
            .args(["-e", "none", "dir-sync", "--remote"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        let mut remote_in = ProstWrite::new(child.stdin.take().unwrap());
        let mut remote_out = ProstRead::new(child.stdout.take().unwrap());
        let remote_err = child.stderr.take().unwrap();

        // 2. check protocol version
        let remote_version = remote_out.recv::<Response>().await?;
        let Some(RemoteRsp::Version(version)) = remote_version.rsp else {
            anyhow::bail!("internal error: expecting RemoteRsp::Version");
        };
        anyhow::ensure!(
            version == REMOTE_PROTOCOL_VERSION,
            "remote protocol version mismatch, need to have compatible versions ont the machines"
        );

        // 3. spawn output watchers
        // expecting a single result
        let (sender_snap, receiver_snap) = flume::bounded(1);
        let (sender_act_req, receiver_act_req) = flume::bounded(1);
        let (sender_act_rsp, receiver_act_rsp) = flume::bounded(1);
        task_tracker.spawn(async move {
            Self::stdout_task(remote_out, sender_snap, sender_act_rsp).await
        })?;
        task_tracker.spawn({
            let fqn = tp.fqn.clone();
            async move { Self::stderr_task(remote_err, fqn).await }
        })?;

        // 4. send configuration
        remote_in
            .send(&Request {
                req: Some(RemoteReq::Config(proto::remote::Config {
                    path: tp.path.clone(),
                    ts: Some(ts),
                    sync_fqns,
                    perf_data_buffer_size: config.perf_data_buffer_size as u64,
                    perf_fs_queue_size: config.perf_fs_queue_size as u32,
                    filter_ignore_name: config.filter_ignore_name.clone(),
                    filter_ignore_path: config.filter_ignore_path.clone(),
                    filter_white_list: config.filter_white_list.clone(),
                })),
            })
            .await?;

        // 5. spawn action sender
        task_tracker.spawn(async move { Self::stdin_task(remote_in, receiver_act_req).await })?;

        Ok(Self {
            child,
            sender_act_req,
            receiver_act_rsp,
            metadata_state: TreeMetadataState::Processing(receiver_snap),
        })
    }

    async fn stdin_task<S: AsyncWriteExt + Unpin>(
        mut remote_in: ProstWrite<S>,
        receiver_act_req: flume::Receiver<Arc<ActionReq>>,
    ) -> anyhow::Result<TaskExit> {
        while let Ok(req) = receiver_act_req.recv_async().await {
            remote_in
                .send(&Request {
                    req: Some(RemoteReq::ActionReq(proto::action::ActionReq {
                        req: Some((*req).clone()),
                    })),
                })
                .await?;
        }
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }

    async fn stdout_task<S: AsyncReadExt + Unpin>(
        mut remote_out: ProstRead<S>,
        sender_snap: flume::Sender<Box<TreeWalkOutput>>,
        sender_act_rsp: flume::Sender<ActionRsp>,
    ) -> anyhow::Result<TaskExit> {
        while let Ok(rsp) = remote_out.recv::<Response>().await {
            match rsp.rsp {
                Some(RemoteRsp::Version(_)) => {
                    anyhow::bail!("internal error: unexpected RemoteRsp Version");
                }
                Some(RemoteRsp::WalkOutput(output)) => {
                    sender_snap.send(Box::new(TreeWalkOutput {
                        snap: output.snap.unwrap(),
                        prev_sync_snap: output.prev_sync_snap,
                    }))?;
                }
                Some(RemoteRsp::ActionRsp(action_rsp)) => {
                    sender_act_rsp.send(action_rsp.rsp.unwrap())?;
                }
                None => {
                    anyhow::bail!("internal error: missing RemoteRsp");
                }
            }
        }
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }

    async fn stderr_task<S: AsyncReadExt + Unpin>(
        mut remote_err: S,
        fqn: String,
    ) -> anyhow::Result<TaskExit> {
        let mut buf = Vec::with_capacity(4096);
        while remote_err.read_buf(&mut buf).await.is_ok() {
            let s = str::from_utf8(&buf).unwrap_or("invalid UTF-8");
            log::error!("tree_remote[{fqn}]: error; {s}");
            buf.clear();
        }
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }
}

impl TreeMetadata for TreeRemote {
    fn get_entry(&self, rel_path: &str) -> Option<&MyDirEntry> {
        let TreeMetadataState::Received(output) = &self.metadata_state else {
            panic!("inconsistent state, call wait_for_tree() first");
        };
        output.snap.get_entry(rel_path)
    }

    fn get_dir_content(&self, rel_path: &str) -> &[MyDirEntry] {
        let Some(entry) = self.get_entry(rel_path) else {
            return &[];
        };
        let Some(Specific::Directory(dir_data)) = &entry.specific else {
            return &[];
        };
        &dir_data.content
    }
}

#[async_trait::async_trait]
impl Tree for TreeRemote {
    async fn wait_for_tree(&mut self) -> anyhow::Result<()> {
        if let TreeMetadataState::Processing(receiver_snap) = &self.metadata_state {
            let snap = receiver_snap.recv_async().await?;
            self.metadata_state = TreeMetadataState::Received(snap);
        }
        Ok(())
    }

    fn get_fs_action_requester(&self) -> ActionReqSender {
        self.sender_act_req.clone()
    }

    fn get_fs_action_responder(&self) -> ActionRspReceiver {
        self.receiver_act_rsp.clone()
    }

    fn save_snap(&mut self, _sync: bool) {
        if let TreeMetadataState::Received(_output) =
            std::mem::replace(&mut self.metadata_state, TreeMetadataState::Terminated)
        {
            //log::info!("tree[{}]: saving snap", self.fs_tree);
            todo!();
        }
    }

    fn take_prev_sync_snap(&mut self) -> Option<MetadataSnap> {
        if let TreeMetadataState::Received(output) = &mut self.metadata_state {
            output.prev_sync_snap.take()
        } else {
            None
        }
    }
}
