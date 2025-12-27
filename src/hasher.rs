//! Hash computation, using blake3 algo

use std::path::PathBuf;

use blake3::Hash;

use crate::generic::task_tracker::{TaskExit, TaskTracker};

/// Input message for `Hasher`
pub struct HasherInput {
    path: PathBuf,
    callback: Box<dyn FnOnce(HasherOutput) + Send + Sync + 'static>,
}

pub type HasherSender = flume::Sender<HasherInput>;

/// Output message of `Hasher`
pub struct HasherOutput {
    path: PathBuf,
    hash: Hash,
}

pub struct Hasher {
    task_tracker: TaskTracker,
    receiver: flume::Receiver<HasherInput>,
}
impl Hasher {
    pub fn spawn(task_tracker: &TaskTracker) -> anyhow::Result<HasherSender> {
        let (sender, receiver) = flume::unbounded();
        let instance = Self {
            task_tracker: task_tracker.clone(),
            receiver,
        };
        task_tracker.spawn_blocking(|| instance.task())?;
        Ok(sender)
    }

    fn task(self) -> anyhow::Result<TaskExit> {
        while let Ok(input) = self.receiver.recv() {
            let mut hasher = blake3::Hasher::new();
            hasher.update_mmap_rayon(&input.path)?;
            let hash = hasher.finalize();
            (input.callback)(HasherOutput {
                path: input.path,
                hash,
            });
        }
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::generic::task_tracker::TaskTrackerMain;

    #[tokio::test]
    async fn test_hasher() -> anyhow::Result<()> {
        // create test dir with some content
        let test_dir = tempfile::tempdir()?;
        let test_dir_path = test_dir.path();
        let file_path = test_dir_path.join("file");
        std::fs::write(&file_path, "content")?;

        // launch Hasher
        let mut task_tracker_main = TaskTrackerMain::default();
        let task_tracker = task_tracker_main.tracker();
        let hasher_sender = Hasher::spawn(&task_tracker)?;
        drop(task_tracker);

        // request file hash
        let (result_sender, result_receiver) = flume::unbounded();
        hasher_sender.send(HasherInput {
            path: file_path.clone(),
            callback: Box::new(move |output| result_sender.send(output).unwrap()),
        })?;
        drop(hasher_sender);

        let result = result_receiver
            .into_iter()
            .map(|output| (output.path, output.hash.to_string()))
            .collect::<Vec<_>>();
        println!("{result:?}");
        assert_eq!(
            result,
            vec![(
                file_path,
                String::from("3fba5250be9ac259c56e7250c526bc83bacb4be825f2799d3d59e5b4878dd74e")
            )]
        );

        task_tracker_main.request_stop();
        task_tracker_main.wait().await?;
        Ok(())
    }
}
