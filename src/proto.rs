//! Protobuf related code

#![allow(
    clippy::doc_lazy_continuation,
    clippy::enum_variant_names,
    clippy::large_enum_variant,
    clippy::pedantic
)]

use std::cmp::Ordering;
use std::path::Path;

// generated code from proto files
include!(concat!(env!("OUT_DIR"), "/mod.rs"));

pub use action::{action_req::Req as ActionReq, action_rsp::Rsp as ActionRsp};
pub use common::{DeviceData, DirectoryData, MyDirEntry, RegularData, my_dir_entry::Specific};
pub use persist::MetadataSnap;
pub use remote::{request::Req as RemoteReq, response::Rsp as RemoteRsp};

pub use prost_types::Timestamp;

/// Null value for google.protobuf.NullValue fields
pub const PROTO_NULL_VALUE: i32 = 0;

/// Version of the protocol (remote.proto) currently implemented
/// Value shall be incremented on incompatible protocol change
pub const REMOTE_PROTOCOL_VERSION: u32 = 1;

/// Extension to `MyDirEntry`
pub trait MyDirEntryExt
where
    Self: Sized,
{
    /// Key for sorting
    fn sort_key(&self) -> &str;

    /// Comparison function
    fn cmp(a: &Self, b: &Self) -> Ordering {
        a.sort_key().cmp(b.sort_key())
    }

    /// Report whether this entry represents a regular file
    fn is_file(&self) -> bool;

    /// Report whether this entry represents a directory
    fn is_dir(&self) -> bool;

    /// Report whether this entry represents a symlink
    fn is_symlink(&self) -> bool;

    /// Get one entry in the tree
    fn get_entry(&self, rel_path: &str) -> Option<&MyDirEntry>;

    /// Get one entry in the tree
    fn get_entry_mut(&mut self, rel_path: &str) -> Option<&mut MyDirEntry>;

    /// Take one entry from the tree
    /// In the tree, the entry is kept (so name is still valid, allowing binary_search)
    /// but specific field is taken
    fn take_entry(&mut self, rel_path: &str) -> Option<MyDirEntry>;

    /// Get content of one directory in the tree (sorted)
    fn get_dir_content(&self, rel_path: &str) -> &[MyDirEntry];

    /// Delete one entry from the tree
    fn delete_entry(&mut self, rel_path: &str);

    /// Create or update one entry in the tree
    ///
    /// Update the existing entry, or create a new one with provided content
    ///
    /// Note: for directories, the specific field (content of the directory)
    /// is moved from the existing to the new entry
    fn create_or_update_entry(&mut self, rel_path: &str, entry: MyDirEntry) -> anyhow::Result<()>;

    /// Get file type as string
    fn type_as_str(&self) -> &'static str;
}

impl MyDirEntryExt for MyDirEntry {
    fn sort_key(&self) -> &str {
        &self.file_name
    }

    fn is_file(&self) -> bool {
        matches!(self.specific, Some(Specific::Regular(_)))
    }

    fn is_dir(&self) -> bool {
        matches!(self.specific, Some(Specific::Directory(_)))
    }

    fn is_symlink(&self) -> bool {
        matches!(self.specific, Some(Specific::Symlink(_)))
    }

    fn get_entry(&self, rel_path: &str) -> Option<&MyDirEntry> {
        let mut entry = self;
        for walk in Path::new(rel_path) {
            if walk == "." {
                continue;
            }
            let d = walk.to_str().unwrap();
            let Some(Specific::Directory(dir_data)) = &entry.specific else {
                return None;
            };
            let index = dir_data
                .content
                .binary_search_by_key(&d, MyDirEntry::sort_key)
                .ok()?;
            entry = &dir_data.content[index];
        }
        Some(entry)
    }

    fn get_entry_mut(&mut self, rel_path: &str) -> Option<&mut MyDirEntry> {
        let mut entry = self;
        for walk in Path::new(rel_path) {
            if walk == "." {
                continue;
            }
            let d = walk.to_str().unwrap();
            let Some(Specific::Directory(dir_data)) = &mut entry.specific else {
                return None;
            };
            let index = dir_data
                .content
                .binary_search_by_key(&d, MyDirEntry::sort_key)
                .ok()?;
            entry = &mut dir_data.content[index];
        }
        Some(entry)
    }

    fn take_entry(&mut self, rel_path: &str) -> Option<MyDirEntry> {
        let src_entry = self.get_entry_mut(rel_path)?;
        Some(MyDirEntry {
            file_name: src_entry.file_name.clone(),
            specific: src_entry.specific.take(),
            ..*src_entry
        })
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

    fn delete_entry(&mut self, rel_path: &str) {
        let path = Path::new(rel_path);
        let parent = path.parent().unwrap().to_str().unwrap();
        let file_name = path.file_name().unwrap().to_str().unwrap();

        // find parent directory
        let Some(entry) = self.get_entry_mut(parent) else {
            return;
        };
        let Some(Specific::Directory(dir_data)) = &mut entry.specific else {
            return;
        };

        // discard entry
        dir_data.content.retain(|e| e.file_name != file_name);
    }

    fn create_or_update_entry(
        &mut self,
        rel_path: &str,
        mut entry: MyDirEntry,
    ) -> anyhow::Result<()> {
        let path = Path::new(rel_path);
        let parent = path.parent().unwrap().to_str().unwrap();
        let file_name = path.file_name().unwrap().to_str().unwrap();

        // find parent directory
        let Some(parent_entry) = self.get_entry_mut(parent) else {
            anyhow::bail!("invalid relative path {rel_path}, cannot find parent directory");
        };
        let Some(Specific::Directory(dir_data)) = &mut parent_entry.specific else {
            anyhow::bail!("invalid relative path {rel_path}, cannot find parent directory");
        };

        // find entry position
        match dir_data
            .content
            .binary_search_by_key(&file_name, MyDirEntry::sort_key)
        {
            Ok(index) => {
                // existing entry
                if dir_data.content[index].is_dir() {
                    // keep content of directory
                    entry.specific = dir_data.content[index].specific.take();
                }
                dir_data.content[index] = entry;
            }
            Err(index) => {
                // entry not existing, insert it
                dir_data.content.insert(index, entry);
            }
        }
        Ok(())
    }

    fn type_as_str(&self) -> &'static str {
        match &self.specific {
            Some(Specific::Fifo(_)) => "Fifo",
            Some(Specific::Character(_)) => "CharDev",
            Some(Specific::Directory(_)) => "Directory",
            Some(Specific::Block(_)) => "BlockDev",
            Some(Specific::Regular(_)) => "Regular",
            Some(Specific::Symlink(_)) => "Symlink",
            Some(Specific::Socket(_)) => "Socket",
            None => "Unknown",
        }
    }
}

// Consider MetadataSnap as a MyDirEntry
impl std::ops::Deref for MetadataSnap {
    type Target = MyDirEntry;

    fn deref(&self) -> &Self::Target {
        self.root.as_ref().expect("invalid MetadataSnap, no root")
    }
}
impl std::ops::DerefMut for MetadataSnap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.root.as_mut().expect("invalid MetadataSnap, no root")
    }
}

/// Extension to `prost_types::Timestamp`
pub trait TimestampExt
where
    Self: Sized,
{
    /// Get current time as timestamp
    fn now() -> Self;
}

impl TimestampExt for Timestamp {
    fn now() -> Self {
        use std::time::SystemTime;
        let d = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        Self {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        }
    }
}

pub trait TimestampOrd {
    fn cmp(&self, other: &Self) -> Ordering;
}

impl TimestampOrd for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seconds
            .cmp(&other.seconds)
            .then(self.nanos.cmp(&other.nanos))
    }
}
// Timestamps are put into in a message, but are always set
impl TimestampOrd for Option<Timestamp> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.unwrap().cmp(other.as_ref().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_file(name: &str) -> MyDirEntry {
        MyDirEntry {
            file_name: name.to_string(),
            specific: Some(Specific::Regular(RegularData {
                size: 123,
                hash: vec![],
            })),
            permissions: 0,
            uid: 0,
            gid: 0,
            mtime: None,
        }
    }

    fn create_dir(name: &str, content: Vec<MyDirEntry>) -> MyDirEntry {
        let mut content = content;
        content.sort_by(MyDirEntry::cmp);
        MyDirEntry {
            file_name: name.to_string(),
            specific: Some(Specific::Directory(DirectoryData { content })),
            permissions: 0,
            uid: 0,
            gid: 0,
            mtime: None,
        }
    }

    fn create_test_tree() -> MyDirEntry {
        create_dir(
            ".",
            vec![
                create_file("file1.txt"),
                create_dir(
                    "subdir1",
                    vec![
                        create_file("subfile1.txt"),
                        create_dir("subdir2", vec![create_file("subsubfile1.txt")]),
                    ],
                ),
                create_file("file2.txt"),
            ],
        )
    }

    #[test]
    fn test_get_entry() {
        let tree = create_test_tree();
        assert_eq!(tree.get_entry(".").unwrap().file_name, ".");
        assert_eq!(
            tree.get_entry("./file1.txt").unwrap().file_name,
            "file1.txt"
        );
        assert_eq!(tree.get_entry("./subdir1").unwrap().file_name, "subdir1");
        assert_eq!(
            tree.get_entry("./subdir1/subfile1.txt").unwrap().file_name,
            "subfile1.txt"
        );
        assert_eq!(
            tree.get_entry("./subdir1/subdir2").unwrap().file_name,
            "subdir2"
        );
        assert_eq!(
            tree.get_entry("./subdir1/subdir2/subsubfile1.txt")
                .unwrap()
                .file_name,
            "subsubfile1.txt"
        );
        assert!(tree.get_entry("nonexistent").is_none());
        assert!(tree.get_entry("./subdir1/nonexistent").is_none());
    }

    #[test]
    fn test_get_entry_mut() {
        let mut tree = create_test_tree();
        let entry = tree.get_entry_mut("./subdir1/subfile1.txt").unwrap();
        assert_eq!(entry.file_name, "subfile1.txt");
        entry.permissions = 123;
        let entry_ro = tree.get_entry("./subdir1/subfile1.txt").unwrap();
        assert_eq!(entry_ro.permissions, 123);
    }

    #[test]
    fn test_get_dir_content() {
        let tree = create_test_tree();
        let root_content = tree.get_dir_content(".");
        assert_eq!(root_content.len(), 3);
        assert_eq!(root_content[0].file_name, "file1.txt");
        assert_eq!(root_content[1].file_name, "file2.txt");
        assert_eq!(root_content[2].file_name, "subdir1");

        let subdir_content = tree.get_dir_content("./subdir1");
        assert_eq!(subdir_content.len(), 2);
        assert_eq!(subdir_content[0].file_name, "subdir2");
        assert_eq!(subdir_content[1].file_name, "subfile1.txt");

        let empty_content = tree.get_dir_content("./nonexistent");
        assert!(empty_content.is_empty());

        let file_content = tree.get_dir_content("./file1.txt");
        assert!(file_content.is_empty());
    }

    #[test]
    fn test_delete_entry() {
        let mut tree = create_test_tree();
        assert!(tree.get_entry("./subdir1/subfile1.txt").is_some());
        tree.delete_entry("./subdir1/subfile1.txt");
        assert!(tree.get_entry("./subdir1/subfile1.txt").is_none());

        let subdir_content = tree.get_dir_content("./subdir1");
        assert_eq!(subdir_content.len(), 1);
        assert_eq!(subdir_content[0].file_name, "subdir2");
    }

    #[test]
    fn test_create_or_update_entry_create() {
        let mut tree = create_test_tree();
        let new_file = create_file("newfile.txt");
        assert!(tree.get_entry("./subdir1/newfile.txt").is_none());
        tree.create_or_update_entry("./subdir1/newfile.txt", new_file)
            .unwrap();
        let entry = tree.get_entry("./subdir1/newfile.txt").unwrap();
        assert_eq!(entry.file_name, "newfile.txt");

        let subdir_content = tree.get_dir_content("./subdir1");
        assert_eq!(subdir_content.len(), 3);
        assert_eq!(subdir_content[0].file_name, "newfile.txt");
    }

    #[test]
    fn test_create_or_update_entry_update() {
        let mut tree = create_test_tree();
        let mut updated_file = create_file("subfile1.txt");
        updated_file.permissions = 456;
        tree.create_or_update_entry("./subdir1/subfile1.txt", updated_file)
            .unwrap();
        let entry = tree.get_entry("./subdir1/subfile1.txt").unwrap();
        assert_eq!(entry.permissions, 456);
    }

    #[test]
    fn test_create_or_update_entry_update_dir() {
        let mut tree = create_test_tree();
        let original_subdir_content_len = tree.get_dir_content("./subdir1").len();
        assert!(original_subdir_content_len > 0);

        // Create a new dir entry to "update" the existing one
        let mut updated_dir = create_dir("subdir1", vec![]);
        updated_dir.permissions = 777;

        tree.create_or_update_entry("./subdir1", updated_dir)
            .unwrap();

        let entry = tree.get_entry("./subdir1").unwrap();
        assert_eq!(entry.permissions, 777);

        // Check that the content of the directory was preserved
        let new_subdir_content = tree.get_dir_content("./subdir1");
        assert_eq!(new_subdir_content.len(), original_subdir_content_len);
        assert!(tree.get_entry("./subdir1/subfile1.txt").is_some());
    }

    #[test]
    fn test_take_entry() {
        let mut tree = create_test_tree();
        let subfile1 = tree.get_entry("./subdir1/subfile1.txt").unwrap();
        assert!(subfile1.is_file());

        let taken_entry = tree.take_entry("./subdir1/subfile1.txt").unwrap();
        assert!(taken_entry.is_file());

        let subfile1_after_take = tree.get_entry("./subdir1/subfile1.txt").unwrap();
        assert!(subfile1_after_take.specific.is_none());
    }

    #[test]
    fn test_is_type() {
        let file = create_file("f");
        let dir = create_dir("d", vec![]);
        let symlink = MyDirEntry {
            file_name: "s".to_string(),
            specific: Some(Specific::Symlink("target".to_string())),
            ..create_file("s")
        };
        assert!(file.is_file());
        assert!(!file.is_dir());
        assert!(!file.is_symlink());

        assert!(!dir.is_file());
        assert!(dir.is_dir());
        assert!(!dir.is_symlink());

        assert!(!symlink.is_file());
        assert!(!symlink.is_dir());
        assert!(symlink.is_symlink());
    }

    #[test]
    fn test_timestamp_ord() {
        let t1 = Timestamp {
            seconds: 100,
            nanos: 0,
        };
        let t2 = Timestamp {
            seconds: 100,
            nanos: 1,
        };
        let t3 = Timestamp {
            seconds: 101,
            nanos: 0,
        };

        assert_eq!(t1.cmp(&t1), Ordering::Equal);
        assert_eq!(t1.cmp(&t2), Ordering::Less);
        assert_eq!(t2.cmp(&t1), Ordering::Greater);
        assert_eq!(t1.cmp(&t3), Ordering::Less);
        assert_eq!(t3.cmp(&t1), Ordering::Greater);

        let ot1 = Some(t1);
        let ot2 = Some(t2);
        assert_eq!(ot1.cmp(&ot1), Ordering::Equal);
        assert_eq!(ot1.cmp(&ot2), Ordering::Less);
        assert_eq!(ot2.cmp(&ot1), Ordering::Greater);
    }
}
