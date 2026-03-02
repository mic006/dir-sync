//! Manage one `Tree` on a remote machine
//!
//! Spawn a dir-sync process via SSH on the remote machine
//! and control this instance

use std::process::Stdio;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt as _, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};

use crate::config::ConfigRef;
use crate::generic::prost_stream::{ProstRead, ProstWrite};
use crate::generic::task_tracker::{TaskExit, TaskTracker};
use crate::proto::remote::{Request, Response};
use crate::proto::{
    self, ActionReq, ActionRsp, MetadataSnap, MyDirEntry, MyDirEntryExt as _, PROTO_NULL_VALUE,
    REMOTE_PROTOCOL_VERSION, RemoteReq, RemoteRsp, Specific, Timestamp,
};
use crate::tree::{
    ActionReqSender, ActionRspReceiver, Tree, TreeMetadata, TreeMetadataState, TreePath,
    TreeWalkOutput,
};

enum RemoteRequest {
    SaveSnap(bool),
    Terminate,
}

#[derive(PartialEq, Debug)]
enum RemoteEvent {
    SnapSaved,
}

pub struct TreeRemote {
    /// Fully qualified name = "hostname:path"
    fqn: String,
    /// Instance of `dir-sync` running on remote over SSH
    child: Child,
    /// Action requester
    sender_act_req: ActionReqSender,
    /// Action responder
    receiver_act_rsp: ActionRspReceiver,
    /// Current state
    metadata_state: TreeMetadataState,
    /// Sender for generic request
    sender_request: flume::Sender<RemoteRequest>,
    /// Receiver for generic response
    receiver_response: flume::Receiver<RemoteEvent>,
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
        let fqn = tp.fqn.clone();
        // 1. spawn instance
        log::info!("tree_remote[{fqn}]: spawning child process over ssh");
        let mut child = Command::new("ssh")
            .arg(&tp.hostname)
            .args(["-e", "none", "dir-sync", "--remote"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        // TODO: add log options if log is requested

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
        let (sender_request, receiver_request) = flume::bounded(2);
        let (sender_response, receiver_response) = flume::bounded(2);

        task_tracker.spawn({
            let fqn = fqn.clone();
            async move {
                Self::stdout_task(
                    fqn,
                    remote_out,
                    sender_snap,
                    sender_act_rsp,
                    sender_response,
                )
                .await
            }
        })?;
        task_tracker.spawn({
            let fqn = fqn.clone();
            async move { Self::stderr_task(fqn, remote_err).await }
        })?;

        // 4. send configuration
        log::debug!("tree_remote[{fqn}]: send configuration");
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
        task_tracker.spawn(async move {
            Self::stdin_task(remote_in, receiver_act_req, receiver_request).await
        })?;

        Ok(Self {
            fqn,
            child,
            sender_act_req,
            receiver_act_rsp,
            metadata_state: TreeMetadataState::Processing(receiver_snap),
            sender_request,
            receiver_response,
        })
    }

    async fn stdin_task<S: AsyncWriteExt + Unpin>(
        mut remote_in: ProstWrite<S>,
        receiver_act_req: flume::Receiver<Arc<ActionReq>>,
        receiver_request: flume::Receiver<RemoteRequest>,
    ) -> anyhow::Result<TaskExit> {
        loop {
            tokio::select! {
                act_req = receiver_act_req.recv_async() => {
                    if let Ok(act_req) = act_req {
                        remote_in
                            .send(&Request {
                                req: Some(RemoteReq::ActionReq(proto::action::ActionReq {
                                    req: Some((*act_req).clone()),
                                })),
                            })
                            .await?;
                    } else {
                        break;
                    }
                }
                gen_req = receiver_request.recv_async() => {
                    if let Ok(gen_req) = gen_req {
                        match gen_req {
                            RemoteRequest::SaveSnap(sync) => {
                                remote_in
                                    .send(&Request {
                                        req: Some(RemoteReq::SaveSnap(sync)),
                                    })
                                    .await?;
                            }
                            RemoteRequest::Terminate => {
                                remote_in
                                    .send(&Request {
                                        req: Some(RemoteReq::Terminate(PROTO_NULL_VALUE)),
                                    })
                                    .await?;
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }

    async fn stdout_task<S: AsyncReadExt + Unpin>(
        fqn: String,
        mut remote_out: ProstRead<S>,
        sender_snap: flume::Sender<Box<TreeWalkOutput>>,
        sender_act_rsp: flume::Sender<ActionRsp>,
        sender_response: flume::Sender<RemoteEvent>,
    ) -> anyhow::Result<TaskExit> {
        while let Ok(rsp) = remote_out.recv::<Response>().await {
            match rsp.rsp {
                Some(RemoteRsp::Version(_)) => {
                    anyhow::bail!("internal error: unexpected RemoteRsp Version");
                }
                Some(RemoteRsp::WalkOutput(output)) => {
                    log::debug!("tree_remote[{fqn}]: walkoutput received");
                    sender_snap.send(Box::new(TreeWalkOutput {
                        snap: output.snap.unwrap(),
                        prev_sync_snap: output.prev_sync_snap,
                    }))?;
                }
                Some(RemoteRsp::ActionRsp(action_rsp)) => {
                    sender_act_rsp.send(action_rsp.rsp.unwrap())?;
                }
                Some(RemoteRsp::SnapSaved(_)) => {
                    log::debug!("tree_remote[{fqn}]: snap saved");
                    sender_response.send(RemoteEvent::SnapSaved)?;
                }
                None => {
                    anyhow::bail!("internal error: missing RemoteRsp");
                }
            }
        }
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }

    async fn stderr_task<S: AsyncReadExt + Unpin>(
        fqn: String,
        remote_err: S,
    ) -> anyhow::Result<TaskExit> {
        let mut remote_err_lines = tokio::io::BufReader::new(remote_err).lines();
        while let Some(line) = remote_err_lines.next_line().await? {
            log::info!("tree_remote[{fqn}]: stderr; {line}");
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

    fn save_snap(&mut self, sync: bool) {
        if let TreeMetadataState::Received(_output) =
            std::mem::replace(&mut self.metadata_state, TreeMetadataState::Terminated)
        {
            if self
                .sender_request
                .send(RemoteRequest::SaveSnap(sync))
                .is_err()
            {
                log::error!("tree_remote[{}]: failed to save snap", self.fqn);
                return;
            }

            // wait for response
            #[allow(clippy::redundant_guards)]
            match self.receiver_response.recv() {
                Ok(response) if response == RemoteEvent::SnapSaved => {
                    // all good
                }
                Ok(unexpected_rsp) => {
                    log::error!(
                        "tree_remote[{}]: unexpected response while expecting SnapSaved response: {unexpected_rsp:?}",
                        self.fqn
                    );
                }
                Err(err) => {
                    log::error!(
                        "tree_remote[{}]: error while expecting SnapSaved response: {err:?}",
                        self.fqn
                    );
                }
            }
        }
    }

    async fn terminate(&mut self) {
        let _ignored = self
            .sender_request
            .send_async(RemoteRequest::Terminate)
            .await;
        todo!("join child process");
    }

    fn take_prev_sync_snap(&mut self) -> Option<MetadataSnap> {
        if let TreeMetadataState::Received(output) = &mut self.metadata_state {
            output.prev_sync_snap.take()
        } else {
            None
        }
    }
}
