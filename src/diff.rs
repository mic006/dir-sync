//! Compare tree and determine delta

use crate::proto::{MyDirEntry, MyDirEntryExt, Specific};
use crate::tree::TreeMetadata;

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct DiffType: u32 {
        /// type difference: entry does not exist in one of the trees
        /// or is of different type (file vs directory, etc)
        const TYPE        = 1 << 0;
        /// content is different, files and symlinks only
        const CONTENT     = 1 << 1;
        const PERMISSIONS = 1 << 2;
        const UID_GID     = 1 << 3;
        /// modification time; only for regular files and symlinks
        const MTIME       = 1 << 4;
    }
}
impl std::fmt::Display for DiffType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const ELEMS: [(DiffType, char); 5] = [
            (DiffType::TYPE, 't'),
            (DiffType::CONTENT, 'c'),
            (DiffType::PERMISSIONS, 'p'),
            (DiffType::UID_GID, 'o'),
            (DiffType::MTIME, 'm'),
        ];
        for (d, c) in ELEMS {
            if self.contains(d) {
                write!(f, "{c}")?;
            } else {
                write!(f, "-")?;
            }
        }
        Ok(())
    }
}

/// Compare 2 entries and determine difference
///
/// mtime is compared only for regular files and symlinks
fn diff_entries(a: &MyDirEntry, b: &MyDirEntry) -> DiffType {
    let mut diff = DiffType::empty();
    let mut check_mtime = false;

    // type and content check
    #[allow(clippy::match_same_arms)]
    match (a.specific.as_ref().unwrap(), b.specific.as_ref().unwrap()) {
        (Specific::Fifo(_), Specific::Fifo(_)) => (),
        (Specific::Character(dev_a), Specific::Character(dev_b))
        | (Specific::Block(dev_a), Specific::Block(dev_b)) => {
            if dev_a != dev_b {
                diff.insert(DiffType::CONTENT);
            }
        }
        (Specific::Directory(_), Specific::Directory(_)) => (),
        (Specific::Regular(regular_data_a), Specific::Regular(regular_data_b)) => {
            if regular_data_a.size != regular_data_b.size
                || regular_data_a.hash != regular_data_b.hash
            {
                diff.insert(DiffType::CONTENT);
            }
            check_mtime = true;
        }
        (Specific::Symlink(target_a), Specific::Symlink(target_b)) => {
            if target_a != target_b {
                diff.insert(DiffType::CONTENT);
            }
            check_mtime = true;
        }
        (Specific::Socket(_), Specific::Socket(_)) => (),
        _ => diff.insert(DiffType::TYPE),
    }

    if a.permissions != b.permissions {
        diff.insert(DiffType::PERMISSIONS);
    }
    if a.uid != b.uid || a.gid != b.gid {
        diff.insert(DiffType::UID_GID);
    }
    if check_mtime && a.mtime != b.mtime {
        diff.insert(DiffType::MTIME);
    }

    diff
}

/// Difference between input entries
#[derive(Debug)]
pub struct DiffEntry {
    /// relative path of entry
    rel_path: String,
    /// entry from each input
    entries: Vec<Option<MyDirEntry>>,
    /// type of difference
    diff: DiffType,
}
impl std::fmt::Display for DiffEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}  {}", self.diff, self.rel_path)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DiffMode {
    /// Get complete diff output
    Output,
    /// Get only a status = existence of some differences
    Status,
}

/// Compare multiple trees and determine differences
///
/// # Returns
/// - []: all trees are similar, no difference found
/// - [first diff]: when mode == Status, report only the first difference found
/// - [all diffs]: when mode == Output, report all differences found
pub fn diff_trees<T: AsRef<dyn TreeMetadata>>(trees: &[T], mode: DiffMode) -> Vec<DiffEntry> {
    let all_trees: u32 = (1 << trees.len()) - 1;
    let mut dir_stack = vec![String::from(".")];
    let mut dir_tmp_stack = vec![];
    let mut diffs = vec![];

    let mut dir_contents: Vec<&[MyDirEntry]> = vec![&[]; trees.len()];
    let mut indexes = vec![0_usize; trees.len()];

    while let Some(rel_path) = dir_stack.pop() {
        log::debug!("[diff]: entering {rel_path}");

        // init
        for i in 0..trees.len() {
            // reset indexes
            indexes[i] = 0;
            // get content
            dir_contents[i] = trees[i].as_ref().get_dir_content(&rel_path);
        }

        // identify diffs
        loop {
            // identify the next file entry, and which trees have it
            let mut file_name: Option<&str> = None;
            let mut involved_trees = 0u32;
            let mut is_dir = false;
            for i in 0..trees.len() {
                let e = dir_contents[i].get(indexes[i]);
                if let Some(e) = e {
                    if let Some(name) = &file_name {
                        if e.file_name.as_str() < *name {
                            file_name = Some(&e.file_name);
                            is_dir = e.is_dir();
                            involved_trees = 1 << i;
                        } else if e.file_name.as_str() == *name {
                            is_dir |= e.is_dir();
                            involved_trees |= 1 << i;
                        }
                    } else {
                        file_name = Some(&e.file_name);
                        is_dir = e.is_dir();
                        involved_trees = 1 << i;
                    }
                }
            }

            if file_name.is_none() {
                // end of directory
                break;
            }

            let file_name = file_name.unwrap();
            let entry_rel_path = rel_path.clone() + "/" + file_name;

            if is_dir {
                // inspect sub-directories
                dir_tmp_stack.push(entry_rel_path.clone());
            }

            // identify the diff
            let mut diff = DiffType::empty();
            {
                if involved_trees != all_trees {
                    // missing entry in at least one tree
                    diff.insert(DiffType::TYPE);
                }
                // compare other entries against the first one
                let first_idx = involved_trees.trailing_zeros() as usize;
                let first_entry = dir_contents[first_idx].get(indexes[first_idx]).unwrap();
                for i in (first_idx + 1)..trees.len() {
                    if (1 << i) & involved_trees != 0 {
                        let entry = dir_contents[i].get(indexes[i]).unwrap();
                        diff |= diff_entries(first_entry, entry);
                    }
                }
            }
            if !diff.is_empty() {
                let diff_entry = DiffEntry {
                    rel_path: entry_rel_path,
                    entries: (0..trees.len())
                        .map(|i| {
                            if (1 << i) & involved_trees != 0 {
                                Some(dir_contents[i].get(indexes[i]).unwrap().clone())
                            } else {
                                None
                            }
                        })
                        .collect(),
                    diff,
                };
                log::debug!("[diff]: {diff_entry}");
                diffs.push(diff_entry);
                if mode == DiffMode::Status {
                    log::debug!("[diff]: status mode, early exit");
                    return diffs;
                }
            }

            // increment indexes for consumed entry
            for (i, index) in indexes.iter_mut().enumerate() {
                if (1 << i) & involved_trees != 0 {
                    *index += 1;
                }
            }
        }

        // sub directories to be inspected next
        for dir in dir_tmp_stack.drain(..).rev() {
            dir_stack.push(dir);
        }
    }

    log::debug!("[diff]: completed");
    diffs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{DirectoryData, MyDirEntry, RegularData, Specific};
    use crate::tree::TreeMetadata;
    use prost_types::Timestamp;

    fn create_file_entry(file_name: &str, size: u64, hash: u64) -> MyDirEntry {
        MyDirEntry {
            file_name: file_name.to_string(),
            specific: Some(Specific::Regular(RegularData {
                size,
                hash: hash.to_be_bytes().to_vec(),
            })),
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            mtime: Some(Timestamp {
                seconds: 1_234_567_890,
                nanos: 0,
            }),
        }
    }

    fn create_dir_entry(file_name: &str, mut content: Vec<MyDirEntry>) -> MyDirEntry {
        content.sort_by(|a, b| a.file_name.cmp(&b.file_name));
        MyDirEntry {
            file_name: file_name.to_string(),
            specific: Some(Specific::Directory(DirectoryData { content })),
            permissions: 0o755,
            uid: 1000,
            gid: 1000,
            mtime: Some(Timestamp {
                seconds: 1_234_567_890,
                nanos: 0,
            }),
        }
    }

    fn create_symlink_entry(file_name: &str, target: &str) -> MyDirEntry {
        MyDirEntry {
            file_name: file_name.to_string(),
            specific: Some(Specific::Symlink(target.to_string())),
            permissions: 0o777,
            uid: 1000,
            gid: 1000,
            mtime: Some(Timestamp {
                seconds: 1_234_567_890,
                nanos: 0,
            }),
        }
    }

    #[test]
    fn test_diff_entries_no_diff() {
        let a = create_file_entry("file.txt", 10, 100);
        let b = create_file_entry("file.txt", 10, 100);
        assert_eq!(diff_entries(&a, &b), DiffType::empty());
    }

    #[test]
    fn test_diff_entries_type_diff() {
        let a = create_file_entry("entry", 10, 100);
        let mut b = create_dir_entry("entry", vec![]);
        b.permissions = a.permissions;
        b.uid = a.uid;
        b.gid = a.gid;
        b.mtime = a.mtime;
        assert_eq!(diff_entries(&a, &b), DiffType::TYPE);
    }

    #[test]
    fn test_diff_entries_content_diff_file() {
        let a = create_file_entry("file.txt", 10, 100);
        let b = create_file_entry("file.txt", 20, 100);
        assert_eq!(diff_entries(&a, &b), DiffType::CONTENT);

        let a = create_file_entry("file.txt", 10, 100);
        let b = create_file_entry("file.txt", 10, 200);
        assert_eq!(diff_entries(&a, &b), DiffType::CONTENT);
    }

    #[test]
    fn test_diff_entries_content_diff_symlink() {
        let a = create_symlink_entry("link", "target_a");
        let b = create_symlink_entry("link", "target_b");
        assert_eq!(diff_entries(&a, &b), DiffType::CONTENT);
    }

    #[test]
    fn test_diff_entries_permissions_diff() {
        let mut a = create_file_entry("file.txt", 10, 100);
        a.permissions = 0o777;
        let b = create_file_entry("file.txt", 10, 100);
        assert_eq!(diff_entries(&a, &b), DiffType::PERMISSIONS);
    }

    #[test]
    fn test_diff_entries_uid_gid_diff() {
        let mut a = create_file_entry("file.txt", 10, 100);
        a.uid = 1001;
        let b = create_file_entry("file.txt", 10, 100);
        assert_eq!(diff_entries(&a, &b), DiffType::UID_GID);

        let mut a = create_file_entry("file.txt", 10, 100);
        a.gid = 1001;
        let b = create_file_entry("file.txt", 10, 100);
        assert_eq!(diff_entries(&a, &b), DiffType::UID_GID);
    }

    #[test]
    fn test_diff_entries_mtime_diff() {
        let mut a = create_file_entry("file.txt", 10, 100);
        a.mtime = Some(Timestamp {
            seconds: 987_654_321,
            nanos: 0,
        });
        let b = create_file_entry("file.txt", 10, 100);
        assert_eq!(diff_entries(&a, &b), DiffType::MTIME);
    }

    #[test]
    fn test_diff_entries_multiple_diffs() {
        let mut a = create_file_entry("file.txt", 20, 200);
        a.permissions = 0o777;
        let b = create_file_entry("file.txt", 10, 100);
        let expected = DiffType::CONTENT | DiffType::PERMISSIONS;
        assert_eq!(diff_entries(&a, &b), expected);
    }

    struct MockTree {
        root: MyDirEntry,
    }

    impl TreeMetadata for MockTree {
        fn get_entry(&self, rel_path: &str) -> Option<&MyDirEntry> {
            self.root.get_entry(rel_path)
        }

        fn get_dir_content(&self, rel_path: &str) -> &[MyDirEntry] {
            if let Some(entry) = self.root.get_entry(rel_path)
                && let Some(Specific::Directory(data)) = &entry.specific
            {
                return &data.content;
            }
            &[]
        }
    }

    impl AsRef<dyn TreeMetadata> for MockTree {
        fn as_ref(&self) -> &(dyn TreeMetadata + 'static) {
            self
        }
    }

    fn create_root(content: Vec<MyDirEntry>) -> MyDirEntry {
        create_dir_entry("dir", content)
    }

    #[test]
    fn test_diff_trees_identical() {
        crate::generic::test::log_init();

        let content = vec![create_file_entry("file.txt", 10, 100)];
        let tree1 = MockTree {
            root: create_root(content.clone()),
        };
        let tree2 = MockTree {
            root: create_root(content),
        };
        let diffs = diff_trees(&[&tree1, &tree2], DiffMode::Output);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_diff_trees_added_file() {
        crate::generic::test::log_init();

        let tree1 = MockTree {
            root: create_root(vec![]),
        };
        let content2 = vec![create_file_entry("file.txt", 10, 100)];
        let tree2 = MockTree {
            root: create_root(content2),
        };
        let diffs = diff_trees(&[&tree1, &tree2], DiffMode::Output);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].rel_path, "./file.txt");
        assert_eq!(diffs[0].diff, DiffType::TYPE);
        assert!(diffs[0].entries[0].is_none());
        assert!(diffs[0].entries[1].is_some());
    }

    #[test]
    fn test_diff_trees_removed_file() {
        crate::generic::test::log_init();

        let content1 = vec![create_file_entry("file.txt", 10, 100)];
        let tree1 = MockTree {
            root: create_root(content1),
        };
        let tree2 = MockTree {
            root: create_root(vec![]),
        };
        let diffs = diff_trees(&[&tree1, &tree2], DiffMode::Output);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].rel_path, "./file.txt");
        assert_eq!(diffs[0].diff, DiffType::TYPE);
        assert!(diffs[0].entries[0].is_some());
        assert!(diffs[0].entries[1].is_none());
    }

    #[test]
    fn test_diff_trees_modified_file() {
        crate::generic::test::log_init();

        let content1 = vec![create_file_entry("file.txt", 10, 100)];
        let tree1 = MockTree {
            root: create_root(content1),
        };
        let content2 = vec![create_file_entry("file.txt", 20, 200)];
        let tree2 = MockTree {
            root: create_root(content2),
        };
        let diffs = diff_trees(&[&tree1, &tree2], DiffMode::Output);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].rel_path, "./file.txt");
        assert_eq!(diffs[0].diff, DiffType::CONTENT);
        assert!(diffs[0].entries[0].is_some());
        assert!(diffs[0].entries[1].is_some());
    }

    #[test]
    fn test_diff_trees_nested() {
        crate::generic::test::log_init();

        let content1 = vec![create_dir_entry(
            "dir",
            vec![create_file_entry("nested.txt", 10, 100)],
        )];
        let tree1 = MockTree {
            root: create_root(content1),
        };
        let content2 = vec![create_dir_entry(
            "dir",
            vec![create_file_entry("nested.txt", 20, 200)],
        )];
        let tree2 = MockTree {
            root: create_root(content2),
        };
        let diffs = diff_trees(&[&tree1, &tree2], DiffMode::Output);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].rel_path, "./dir/nested.txt");
        assert_eq!(diffs[0].diff, DiffType::CONTENT);
    }

    #[test]
    fn test_diff_trees_three_way() {
        crate::generic::test::log_init();

        let content1 = vec![create_file_entry("file.txt", 10, 100)];
        let tree1 = MockTree {
            root: create_root(content1),
        };
        let content2 = vec![create_file_entry("file.txt", 20, 200)];
        let tree2 = MockTree {
            root: create_root(content2),
        };
        let tree3 = MockTree {
            root: create_root(vec![]),
        };
        let diffs = diff_trees(&[&tree1, &tree2, &tree3], DiffMode::Output);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].rel_path, "./file.txt");
        assert_eq!(diffs[0].diff, DiffType::TYPE | DiffType::CONTENT);
        assert!(diffs[0].entries[0].is_some());
        assert!(diffs[0].entries[1].is_some());
        assert!(diffs[0].entries[2].is_none());
    }
}
