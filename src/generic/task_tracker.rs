//! Task tracker helps to manage tokio tasks and have a clean shutdown.

use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;

use futures::{StreamExt, stream::FuturesUnordered};
use tokio::task::JoinHandle;

/// Specific error to exit early when shutdown has been requested
/// Shall be considered as an error if application shutdown has not been requested.
/// Otherwise it is considered as an expected error and ignored.
#[derive(Debug)]
pub struct ShutdownReqError;
impl std::error::Error for ShutdownReqError {}
impl std::fmt::Display for ShutdownReqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Shutdown requested, early exit")
    }
}

/// Exit status for all tracked tasks
pub enum TaskExit {
    /// Main task has exited successfully, application shall stop
    MainTaskStopAppSuccess,
    /// Main task has exited with failure, application shall stop with provided exit code
    MainTaskStopAppFailure(ExitCode),
    /// Secondary task exited, application shall keep running
    SecondaryTaskKeepRunning,
}

/// Return type of all tracked tasks
pub type TrackedTaskResult = anyhow::Result<TaskExit>;

/// Common join handle for all tracked tasks
type TaskJoinHandle = JoinHandle<TrackedTaskResult>;

/// Generic function to allow conditional futures in a `tokio::select!`
///
/// # Usage
/// 1. Wrap the conditional future in an Option<>: Some(fut) when future is valid, None, when invalid
/// 2. In `tokio::select!`, use the following branch: `Some(r) = option_wait(opt_fut) => ...`
///
async fn option_wait<F>(opt_fut: Option<F>) -> Option<F::Output>
where
    F: Future,
{
    match opt_fut {
        Some(fut) => Some(fut.await),
        None => None,
    }
}

/// Return an Option<future> to receive on the given channel if it is still valid.
///
/// # Returns
/// * `Some(rx.recv_async())` while there may be some content to receive
/// * `None` when the channel is closed and empty
fn option_recv<T>(rx: &flume::Receiver<T>) -> Option<flume::r#async::RecvFut<'_, T>> {
    if rx.is_disconnected() && rx.is_empty() {
        None
    } else {
        Some(rx.recv_async())
    }
}

/// Return an Option<future> to wait for a future in the future vector.
///
/// # Returns
/// * `Some(fut.next())` while there at least one future in the vector
/// * `None` when the vector is empty
fn option_futures<F: Future>(
    futures: &mut FuturesUnordered<F>,
) -> Option<futures::stream::Next<'_, FuturesUnordered<F>>> {
    if futures.is_empty() {
        None
    } else {
        Some(futures.next())
    }
}

/// State of `TaskTrackerMain`
#[derive(Debug)]
enum State {
    /// Application is running normally
    Running,
    /// Application is exiting normally
    ExitSuccess,
    /// Application is exiting abnormally, with specific exit code
    ExitFailure(ExitCode),
    /// Application is exiting due to the given error
    ExitError(anyhow::Error),
    /// Task tracker completed via `TaskTrackerMain::wait()`
    Completed,
}
impl State {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed)
    }
}

/// Main instance of the task tracker
///
/// It tracks all the tasks spawned by any `TaskTracker`,
/// and allows to wait for them or stop them all in case of error.
///
/// # Integration
///
/// All tasks shall be spawned via `TaskTracker::spawn()`.
/// One task shall call `TaskTrackerMain::wait()` to ensure tasks clean-up
/// and shutdown procedure when one task reports an error.
pub struct TaskTrackerMain {
    /// Receive join handles for tasks spawned by any `TaskTracker`
    task_handles_rx: flume::Receiver<TaskJoinHandle>,
    /// Send join handles, needed to create `TaskTracker` instances
    task_handles_tx: Option<flume::Sender<TaskJoinHandle>>,
    /// Tracked tasks
    tasks: FuturesUnordered<TaskJoinHandle>,
    /// Flag used by synchronous / blocking tasks to detect an exit condition
    exit_requested: Arc<std::sync::atomic::AtomicBool>,
    /// State of the task tracker
    state: State,
}
impl Drop for TaskTrackerMain {
    fn drop(&mut self) {
        assert!(
            self.state.is_completed(),
            "Internal error: misuse of TaskTrackerMain. TaskTrackerMain::wait() shall be called by the main task."
        );
    }
}
impl Default for TaskTrackerMain {
    fn default() -> Self {
        let (tx, rx) = flume::unbounded();
        Self {
            task_handles_rx: rx,
            task_handles_tx: Some(tx),
            tasks: FuturesUnordered::default(),
            exit_requested: Arc::default(),
            state: State::Running,
        }
    }
}
impl TaskTrackerMain {
    /// Register signals handler and stop the application on received signal
    ///
    /// # Errors
    /// * cannot spawn task
    pub fn setup_signal_catching(&self) -> anyhow::Result<()> {
        self.tracker()
            .spawn(TaskTrackerMain::task_signal_catching())
    }

    /// Request stop of all tasks
    pub fn request_stop(&mut self) {
        self.state = State::ExitSuccess;
        self.abort();
    }

    /// Wait for completion of all tasks
    ///
    /// - if any task is completed with `TaskExit::MainTaskStopApp` or with an error:
    ///   - abort all the remaining tasks,
    ///   - wait for their completion
    ///   - report success if all is fine, else the first error encountered
    /// - otherwise, continue monitoring the different tasks
    ///
    /// Note: new tasks may be spawned while wait is performed; they will be monitored as well
    ///
    /// # Returns
    /// * Ok(SUCCESS) when no error has been reported
    /// * Ok(signum) when the exit has been triggered by a signal
    /// * Err(_) error reported by the first failing task
    ///
    /// # Errors
    /// * error reported by any task of the program
    pub async fn wait(mut self) -> anyhow::Result<ExitCode> {
        // close our copy of task_handles_tx
        drop(self.task_handles_tx.take());

        // wait for
        // - self.task_handles_rx to be exhausted = no sender alive and channel empty
        // - AND self.tasks to be exhausted = no living task
        loop {
            // both sources may be unavailable at some point
            // - there may be no task yet in self.tasks, as the handles have not been received and processed yet
            // - the task_handles_rx channel may be closed
            // Use Option<> to perform a select! on the first, second or both futures depending on their availability
            let task_handles_rx = option_recv(&self.task_handles_rx);
            let tasks = option_futures(&mut self.tasks);
            if task_handles_rx.is_none() && tasks.is_none() {
                // job done
                break;
            }
            tokio::select! {
                Some(res_handle) = option_wait(task_handles_rx) => {
                    if let Ok(handle) = res_handle {
                        if ! self.state.is_running() {
                            // application is already in exiting phase: abort this new task
                            handle.abort();
                        }
                        // keep track of it
                        self.tasks.push(handle);
                    }
                    // else: channel has been closed
                },
                Some(res_next) = option_wait(tasks) => {
                    let res_join = res_next.expect("safe: self.tasks cannot be empty");
                    match res_join {
                        Ok(task_res) => self.handle_task_result(task_res),
                        Err(join_err) if join_err.is_panic()  => {
                            // allow clean drop() of TaskTracker
                            self.state = State::Completed;
                            // propagate task panic
                            std::panic::resume_unwind(join_err.into_panic());
                        },
                        Err(_join_err) /* cancelled task */ => (),
                    }
                },
            }
        }
        log::debug!("All tasks are terminated");

        match std::mem::replace(&mut self.state, State::Completed) {
            State::ExitSuccess => Ok(ExitCode::SUCCESS),
            State::ExitFailure(exit_code) => Ok(exit_code),
            State::ExitError(err) => Err(err),
            _ => unreachable!(),
        }
    }

    /// Get clone-able tracker
    #[must_use]
    pub fn tracker(&self) -> TaskTracker {
        TaskTracker {
            task_handles_tx: self.task_handles_tx.as_ref().expect("safe").clone(),
            exit_requested: self.exit_requested.clone(),
        }
    }

    /// Spawn tokio async task, and track it
    ///
    /// # Errors
    /// * cannot spawn task
    pub fn spawn<F>(&self, fut: F) -> anyhow::Result<()>
    where
        F: Future<Output = TrackedTaskResult> + Send + 'static,
    {
        Ok(self
            .task_handles_tx
            .as_ref()
            .unwrap()
            .send(tokio::spawn(fut))?)
    }

    /// Task to receive and process signal
    async fn task_signal_catching() -> TrackedTaskResult {
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("sigint setup failure");
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("sigterm setup failure");
        let mut sigpipe = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::pipe())
            .expect("sigpipe setup failure");
        let (sig_num, sig_name) = tokio::select! {
            _ = sigint.recv() => (2, "SIGINT"),
            _ = sigterm.recv() => (15, "SIGTERM"),
            _ = sigpipe.recv() => (13, "SIGPIPE"),
        };
        log::warn!("exiting due to signal {sig_name}");
        Ok(TaskExit::MainTaskStopAppFailure(ExitCode::from(
            128 + sig_num,
        )))
    }

    /// Handle ended task result: update state and abort tasks if needed
    fn handle_task_result(&mut self, res: TrackedTaskResult) {
        match self.state {
            State::Running => {
                // set exit state based on event
                self.state = match res {
                    Ok(TaskExit::MainTaskStopAppSuccess) => State::ExitSuccess,
                    Ok(TaskExit::MainTaskStopAppFailure(exit_code)) => {
                        State::ExitFailure(exit_code)
                    }
                    Ok(TaskExit::SecondaryTaskKeepRunning) => return, // no exit
                    Err(err) => State::ExitError(err),
                };
                // exiting, abort all tasks
                self.abort();
            }
            State::ExitSuccess => {
                // record error if any during shutdown
                #[allow(clippy::match_same_arms)]
                match res {
                    Ok(TaskExit::MainTaskStopAppSuccess) => (),
                    Ok(TaskExit::MainTaskStopAppFailure(exit_code)) => {
                        self.state = State::ExitFailure(exit_code);
                    }
                    Ok(TaskExit::SecondaryTaskKeepRunning) => (),
                    Err(err) => self.state = State::ExitError(err),
                }
            }
            _ => (), // keep the first error condition
        }
    }

    /// Abort all tasks
    fn abort(&self) {
        // inform sync tasks
        self.exit_requested
            .store(true, std::sync::atomic::Ordering::Relaxed);
        // abort async tasks
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[derive(Clone)]
/// Clone-able instance of `TaskTracker`
pub struct TaskTracker {
    /// Send join handles, needed to create `TaskTracker` instance
    task_handles_tx: flume::Sender<TaskJoinHandle>,
    /// Flag used by synchronous / blocking tasks to detect an exit condition
    exit_requested: Arc<std::sync::atomic::AtomicBool>,
}
impl TaskTracker {
    /// Get shutdown request status as a boolean
    ///
    /// Returns: `true` when shutdown is requested, `false` otherwise
    #[must_use]
    pub fn get_shutdown_req_as_bool(&self) -> bool {
        self.exit_requested
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get shutdown request status as a result
    ///
    /// Returns: `Err(ShutdownReqError)` when shutdown is requested, `Ok(())` otherwise
    ///
    /// # Errors
    /// * shutdown has been requested
    pub fn get_shutdown_req_as_result(&self) -> Result<(), ShutdownReqError> {
        if self.get_shutdown_req_as_bool() {
            Err(ShutdownReqError)
        } else {
            Ok(())
        }
    }

    /// Spawn tokio async task, and track it
    ///
    /// # Errors
    /// * cannot spawn task
    pub fn spawn<F>(&self, fut: F) -> anyhow::Result<()>
    where
        F: Future<Output = TrackedTaskResult> + Send + 'static,
    {
        Ok(self.task_handles_tx.send(tokio::spawn(fut))?)
    }

    /// Spawn tokio sync task, and track it
    ///
    /// # Errors
    /// * cannot spawn task
    pub fn spawn_blocking<F>(&self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce() -> TrackedTaskResult + Send + 'static,
    {
        Ok(self.task_handles_tx.send(tokio::task::spawn_blocking(f))?)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn shutdown_sync_and_async() -> anyhow::Result<()> {
        crate::generic::test::log_init();
        let task_tracker_main = TaskTrackerMain::default();
        let task_tracker = task_tracker_main.tracker();

        // spawn sync tasks in dedicated threads
        task_tracker.spawn_blocking({
            let task_tracker = task_tracker.clone();
            move || {
                while !task_tracker.get_shutdown_req_as_bool() {
                    log::info!("Task #sync1: sleeping");
                    std::thread::sleep(Duration::from_millis(10));
                }
                log::info!("Task #sync1: exiting");
                Ok(TaskExit::SecondaryTaskKeepRunning)
            }
        })?;
        task_tracker.spawn_blocking({
            let task_tracker = task_tracker.clone();
            move || loop {
                task_tracker.get_shutdown_req_as_result()?;
                log::info!("Task #sync2: sleeping");
                std::thread::sleep(Duration::from_millis(12));
            }
        })?;

        // spawn an async task
        task_tracker.spawn(async {
            loop {
                log::info!("Task #async1: sleeping");
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        })?;

        // spawn an async task that will fail
        task_tracker.spawn(async {
            log::info!("Task #async2: sleeping");
            tokio::time::sleep(Duration::from_millis(50)).await;
            log::info!("Task #async2: reporting failure");
            anyhow::bail!("Failure reported by sub task")
        })?;

        drop(task_tracker);

        let res = task_tracker_main.wait().await;

        let Err(err) = res else {
            panic!("expecting error")
        };
        assert_eq!(err.to_string(), "Failure reported by sub task");
        Ok(())
    }
}
