//! Main handler for remote instance

use std::sync::Arc;

use crate::config::{Config, ConfigCtx};
use crate::generic::prost_stream::{ProstRead, ProstWrite};
use crate::generic::task_tracker::{TaskExit, TaskTracker, TrackedTaskResult};
use crate::proto::action::ActionRsp;
use crate::proto::remote::{Request, Response, WalkOutput};
use crate::proto::{REMOTE_PROTOCOL_VERSION, RemoteReq, RemoteRsp};
use crate::tree::{Tree as _, TreeMetadata as _};
use crate::tree_local::TreeLocal;

/// Main function for remote instance
///
/// # Errors
/// - IO error
/// - flow error (internal error, sequence of messages)
/// - early exit
pub async fn remote_main(task_tracker: TaskTracker) -> TrackedTaskResult {
    let local_config = Config::from_file(None)?;

    let mut remote_in = ProstRead::new(tokio::io::stdin());
    let mut remote_out = ProstWrite::new(tokio::io::stdout());

    // 1. publish protocol version
    remote_out
        .send(&Response {
            rsp: Some(RemoteRsp::Version(REMOTE_PROTOCOL_VERSION)),
        })
        .await?;

    // 2. wait for configuration
    let master_config: Request = remote_in.recv().await?;
    let Some(RemoteReq::Config(mut master_config)) = master_config.req else {
        anyhow::bail!("internal error: expecting configuration RemoteReq");
    };
    let path = std::mem::take(&mut master_config.path);
    let ts = master_config
        .ts
        .ok_or_else(|| anyhow::anyhow!("internal error: missing ts field in Config"))?;
    let sync_paths = std::mem::take(&mut master_config.sync_paths);
    let config = Arc::new(ConfigCtx::from_master_config(local_config, master_config)?);

    // 3. spawn TreeLocal
    let mut tree = TreeLocal::spawn(config, &task_tracker, &path, ts, sync_paths)?;

    // 4. wait for completion
    tokio::select! {
        res = tree.wait_for_tree() => res?,
        _req = remote_in.recv::<Request>() => {
            // early termination
            return Ok(TaskExit::MainTaskStopAppSuccess);
        }
    }

    // 5. report tree walk completion
    remote_out
        .send(&Response {
            rsp: Some(RemoteRsp::WalkOutput(WalkOutput {
                snap: tree.get_entry(".").cloned(),
                prev_sync_snap: tree.take_prev_sync_snap(),
            })),
        })
        .await?;

    // 6. manage tree responses, forward to stdout
    task_tracker.spawn({
        let tree_action_response = tree.get_fs_action_responder();
        async move {
            while let Ok(rsp) = tree_action_response.recv_async().await {
                remote_out
                    .send(&Response {
                        rsp: Some(RemoteRsp::ActionRsp(ActionRsp { rsp: Some(rsp) })),
                    })
                    .await?;
            }
            Ok(TaskExit::SecondaryTaskKeepRunning)
        }
    })?;

    // 7. dispatch actions
    let tree_action_sender = tree.get_fs_action_requester();
    loop {
        let req = remote_in.recv::<Request>().await?;
        match req.req {
            Some(RemoteReq::Config(_)) => {
                anyhow::bail!("internal error: unexpected RemoteReq Config after initialization");
            }
            Some(RemoteReq::ActionReq(req)) => {
                if let Some(req) = req.req {
                    tree_action_sender.send_async(Arc::new(req)).await?;
                } else {
                    anyhow::bail!("internal error: missing ActionReq");
                }
            }
            Some(RemoteReq::Terminate(_)) => break,
            None => anyhow::bail!("internal error: missing RemoteReq"),
        }
    }

    // 8. clean exit
    tree.save_snap(true);

    Ok(TaskExit::MainTaskStopAppSuccess)
}
