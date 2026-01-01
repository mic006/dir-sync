//! Directory walker

use std::path::{Path, PathBuf};

use crate::config::FileMatcher;
use crate::generic::task_tracker::{TaskExit, TaskTracker};
use crate::proto::{self, MyDirContent, MyDirEntry, MyDirEntryExt as _};

pub type DirWalkReceiver = flume::Receiver<MyDirContent>;

/// Walk a local directory
/// Output the content of each directory:
/// - start from root
/// - then sub-directories of the current dir, in ascending `file_name`
/// - discard filtered entries ASAP (avoid entering the directories)
pub struct DirWalk {
    /// root of directory to walk
    base: PathBuf,
    /// Determine name/paths to be ignored
    file_matcher: Option<FileMatcher>,
    /// content sender
    sender: flume::Sender<MyDirContent>,
}
impl DirWalk {
    pub fn spawn(
        task_tracker: &TaskTracker,
        path: &Path,
        file_matcher: Option<FileMatcher>,
    ) -> anyhow::Result<DirWalkReceiver> {
        let (sender, receiver) = flume::unbounded();
        let instance = Self {
            base: path.into(),
            file_matcher,
            sender,
        };
        task_tracker.spawn_blocking(|| instance.task())?;
        Ok(receiver)
    }

    fn task(self) -> anyhow::Result<TaskExit> {
        let mut dir_stack = vec![PathBuf::new()];

        while let Some(rel_path) = dir_stack.pop() {
            let full_path = self.base.join(&rel_path);
            let mut entries = std::fs::read_dir(&full_path)?
                // filter ignored entries ASAP
                .filter_map(|e| match e {
                    Ok(e) => {
                        let e_rel_path = rel_path.join(e.file_name());
                        if self.file_matcher.as_ref().is_some_and(|fm| {
                            fm.is_ignored(e.file_name().to_str().unwrap(), &e_rel_path)
                        }) {
                            // ignored entry
                            None
                        } else {
                            Some(Ok(e))
                        }
                    }
                    Err(err) => Some(Err(err)),
                })
                .collect::<std::io::Result<Vec<_>>>()?;

            entries.sort_by_key(std::fs::DirEntry::file_name);
            let entries = entries
                .into_iter()
                .map(MyDirEntry::try_from_std_fs)
                .collect::<anyhow::Result<Vec<_>>>()?;
            for e in entries.iter().rev() {
                if e.file_type() == proto::MY_FILE_TYPE_DIRECTORY {
                    dir_stack.push(rel_path.join(&e.file_name));
                }
            }
            self.sender.send(MyDirContent {
                rel_path: rel_path.to_string_lossy().to_string(),
                entries,
            })?;
        }
        Ok(TaskExit::SecondaryTaskKeepRunning)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::tests::load_ut_cfg;
    use crate::generic::task_tracker::TaskTrackerMain;

    #[tokio::test]
    async fn test_dir_walk() -> anyhow::Result<()> {
        // create test dir with some content
        let test_dir = tempfile::tempdir()?;
        let test_dir_path = test_dir.path();
        for p in [
            "sub_folder1/empty",
            "sub_folder1/bar",
            "sub folder 2 €",
            "empty_folder",
        ] {
            std::fs::create_dir_all(test_dir_path.join(p))?;
        }
        std::fs::write(test_dir_path.join("some file"), "alpha")?;
        std::fs::write(test_dir_path.join("empty file"), "")?;
        std::fs::write(test_dir_path.join("sub folder 2 €/beta"), "beta")?;
        std::fs::write(test_dir_path.join("sub_folder1/bar/ignored"), "ignored")?;
        std::fs::write(test_dir_path.join("sub_folder1/ignored~"), "ignored")?;

        let cfg = load_ut_cfg().unwrap();
        let file_matcher = cfg.get_file_matcher("data").unwrap();

        // walk the directory
        let mut task_tracker_main = TaskTrackerMain::default();
        let task_tracker = task_tracker_main.tracker();
        let receiver = DirWalk::spawn(&task_tracker, test_dir_path, Some(file_matcher))?;
        drop(task_tracker);

        let received = receiver
            .into_iter()
            .map(|dc| {
                (
                    dc.rel_path,
                    dc.entries
                        .into_iter()
                        .map(|e| {
                            let file_type = e.file_type();
                            (e.file_name, file_type)
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        println!("{received:?}");
        assert_eq!(
            received,
            vec![
                (
                    String::new(),
                    vec![
                        (String::from("empty file"), proto::MY_FILE_TYPE_REGULAR),
                        (String::from("empty_folder"), proto::MY_FILE_TYPE_DIRECTORY),
                        (String::from("some file"), proto::MY_FILE_TYPE_REGULAR),
                        (
                            String::from("sub folder 2 €"),
                            proto::MY_FILE_TYPE_DIRECTORY
                        ),
                        (String::from("sub_folder1"), proto::MY_FILE_TYPE_DIRECTORY)
                    ]
                ),
                (String::from("empty_folder"), vec![]),
                (
                    String::from("sub folder 2 €"),
                    vec![(String::from("beta"), proto::MY_FILE_TYPE_REGULAR)]
                ),
                (
                    String::from("sub_folder1"),
                    vec![(String::from("empty"), proto::MY_FILE_TYPE_DIRECTORY)]
                ),
                (String::from("sub_folder1/empty"), vec![])
            ]
        );

        task_tracker_main.request_stop();
        task_tracker_main.wait().await?;
        Ok(())
    }
}
