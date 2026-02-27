//! Main handler for remote instance

use crate::generic::prost_stream::{ProstRead, ProstWrite};
use crate::generic::task_tracker::{TaskTracker, TrackedTaskResult};
use crate::proto::remote::{Request, Response};
use crate::proto::{REMOTE_PROTOCOL_VERSION, RemoteReq, RemoteRsp};

/// Main function for remote instance
///
/// # Errors
/// - IO error
/// - flow error (internal error, sequence of messages)
/// - early exit
pub async fn remote_main(_task_tracker: TaskTracker) -> TrackedTaskResult {
    let mut remote_in = ProstRead::new(tokio::io::stdin());
    let mut remote_out = ProstWrite::new(tokio::io::stdout());

    // 1. publish protocol version
    remote_out
        .send(&Response {
            rsp: Some(RemoteRsp::Version(REMOTE_PROTOCOL_VERSION)),
        })
        .await?;

    // 2. wait for configuration
    let cfg: Request = remote_in.recv().await?;
    let Some(RemoteReq::Config(_cfg)) = cfg.req else {
        anyhow::bail!("internal error: expecting configuration RemoteReq");
    };

    // 3. spawn TreeLocal
    //let mut tree = TreeLocal::spawn(config, &task_tracker, path, ts, file_matcher, sync_paths)?;

    // 4. wait for completion

    // 5. dispatch actions

    Ok(crate::generic::task_tracker::TaskExit::MainTaskStopAppSuccess)
}
