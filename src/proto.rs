//! Protobuf related code

#![allow(
    clippy::enum_variant_names,
    clippy::large_enum_variant,
    clippy::pedantic
)]

use std::cmp::Ordering;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
use std::path::{Path, PathBuf};

// generated code from proto files
include!(concat!(env!("OUT_DIR"), "/mod.rs"));

pub use action::{action_req::Req as ActionReq, action_rsp::Rsp as ActionRsp};
pub use common::{DeviceData, DirectoryData, MyDirEntry, RegularData, my_dir_entry::Specific};
pub use persist::MetadataSnap;
pub use prost_types::Timestamp;

use crate::config::ConfigRef;
use crate::generic::fs::{PathExt, systemd_escape_path};

/// Null value for google.protobuf.NullValue fields
pub const PROTO_NULL_VALUE: i32 = 0;

/// Extension to `MyDirEntry`
pub trait MyDirEntryExt
where
    Self: Sized,
{
    /// Convert standard library entry to `MyDirEntry`
    fn try_from_std_fs(value: std::fs::DirEntry) -> anyhow::Result<Self>;

    /// Create entry from metadata content
    fn try_from_metadata(path: &Path, metadata: std::fs::Metadata) -> anyhow::Result<Self>;

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

    /// Get one entry in the tree
    fn get_entry(&self, rel_path: &Path) -> Option<&MyDirEntry>;

    /// Get one entry in the tree
    fn get_entry_mut(&mut self, rel_path: &Path) -> Option<&mut MyDirEntry>;

    /// Insert content in the tree
    fn insert(
        &mut self,
        // relative path of directory to root directory, empty if root
        rel_path: &Path,
        // directory content, unsorted
        entries: Vec<MyDirEntry>,
    ) -> anyhow::Result<()>;
}

impl MyDirEntryExt for MyDirEntry {
    fn try_from_std_fs(value: std::fs::DirEntry) -> anyhow::Result<Self> {
        Self::try_from_metadata(&value.path(), value.metadata()?)
    }

    fn try_from_metadata(path: &Path, metadata: std::fs::Metadata) -> anyhow::Result<Self> {
        /// Build `DeviceData` from rdev value
        fn build_device_data(rdev: u64) -> DeviceData {
            /// Get major id from rdev value
            fn major(rdev: u64) -> u32 {
                // code from Glibc bits/sysmacros.h
                let mut major = ((rdev & 0x0000_0000_000f_ff00_u64) >> 8) as u32;
                major |= ((rdev & 0xffff_f000_0000_0000_u64) >> 32) as u32;
                major
            }
            /// Get minor id from rdev value
            fn minor(rdev: u64) -> u32 {
                // code from Glibc bits/sysmacros.h
                let mut minor = (rdev & 0x0000_0000_0000_00ff_u64) as u32;
                minor |= ((rdev & 0x0000_0fff_fff0_0000_u64) >> 12) as u32;
                minor
            }
            DeviceData {
                major: major(rdev),
                minor: minor(rdev),
            }
        }

        let fs_file_type = metadata.file_type();
        let specific = if fs_file_type.is_fifo() {
            Specific::Fifo(PROTO_NULL_VALUE)
        } else if fs_file_type.is_char_device() {
            Specific::Character(build_device_data(metadata.rdev()))
        } else if fs_file_type.is_dir() {
            // content is unknown at this time
            Specific::Directory(DirectoryData { content: vec![] })
        } else if fs_file_type.is_block_device() {
            Specific::Block(build_device_data(metadata.rdev()))
        } else if fs_file_type.is_file() {
            Specific::Regular(RegularData {
                size: metadata.size(),
                hash: Default::default(),
            })
        } else if fs_file_type.is_symlink() {
            Specific::Symlink(
                path.read_link()?
                    .to_str()
                    .ok_or_else(|| {
                        anyhow::anyhow!("Invalid UTF-8 for symlink at '{}'", path.display())
                    })?
                    .into(),
            )
        } else if fs_file_type.is_socket() {
            Specific::Socket(PROTO_NULL_VALUE)
        } else {
            anyhow::bail!("Unsupported file type at '{}'", path.display());
        };
        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("Invalid path '{}'", path.display()))?
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 for file name at '{}'", path.display()))?
            .into();
        let mtime = Some(Timestamp {
            seconds: metadata.mtime(),
            nanos: metadata.mtime_nsec() as i32,
        });
        Ok(Self {
            file_name,
            permissions: metadata.mode() & 0xFFF,
            uid: metadata.uid(),
            gid: metadata.gid(),
            mtime,
            specific: Some(specific),
        })
    }

    fn sort_key(&self) -> &str {
        &self.file_name
    }

    fn is_file(&self) -> bool {
        matches!(self.specific, Some(Specific::Regular(_)))
    }

    fn is_dir(&self) -> bool {
        matches!(self.specific, Some(Specific::Directory(_)))
    }

    fn get_entry(&self, rel_path: &Path) -> Option<&MyDirEntry> {
        let mut entry = self;
        for walk in rel_path {
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

    fn get_entry_mut(&mut self, rel_path: &Path) -> Option<&mut MyDirEntry> {
        let mut entry = self;
        for walk in rel_path {
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

    fn insert(&mut self, rel_path: &Path, mut entries: Vec<MyDirEntry>) -> anyhow::Result<()> {
        let dir = self.get_entry_mut(rel_path).ok_or_else(|| {
            anyhow::anyhow!(
                "inconsistent snapshot, directory {} not found",
                rel_path.display()
            )
        })?;

        entries.sort_by(MyDirEntry::cmp);

        let Some(Specific::Directory(dir_data)) = &mut dir.specific else {
            anyhow::bail!(
                "inconsistent snapshot, trying to insert entries in a non-directory entry"
            );
        };
        anyhow::ensure!(
            dir_data.content.is_empty(),
            "inconsistent snapshot, trying to insert entries in a non-empty entry"
        );
        dir_data.content = entries;

        Ok(())
    }
}

/// Extension to `MetadataSnap`
pub trait MetadataSnapExt
where
    Self: Sized,
{
    /// Create new instance for given root
    fn new(path: &Path) -> anyhow::Result<Self>;
}

impl MetadataSnapExt for MetadataSnap {
    fn new(path: &Path) -> anyhow::Result<Self> {
        let root = MyDirEntry::try_from_metadata(path, std::fs::metadata(path)?)?;
        Ok(Self {
            ts: Some(Timestamp::now()),
            root: Some(root),
        })
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

/// Convert canonical path to snap file name
pub fn get_metadata_snap_path(cfg: &ConfigRef, input_path: &Path) -> PathBuf {
    let mut path = cfg
        .local_metadata_snap_path_user
        .join(systemd_escape_path(input_path.checked_as_str().unwrap()));
    path.set_extension("pb.bin.zst");
    path
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::config::tests::load_ut_cfg;

    use super::*;

    #[test]
    fn test_metadata_snap() -> anyhow::Result<()> {
        // create test dir with some content
        let test_dir = tempfile::tempdir()?;
        let test_dir_path = test_dir.path();
        std::fs::create_dir(test_dir_path.join("sub_folder"))?;
        std::fs::write(test_dir_path.join("sub_folder/beta"), "")?;
        std::fs::write(test_dir_path.join("alpha"), "")?;
        std::os::unix::fs::symlink("target", test_dir_path.join("some_link"))?;

        let root_entries = std::fs::read_dir(test_dir_path)?
            .flat_map(|e| e.map(MyDirEntry::try_from_std_fs))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let sub_entries = std::fs::read_dir(test_dir_path.join("sub_folder"))?
            .flat_map(|e| e.map(MyDirEntry::try_from_std_fs))
            .collect::<anyhow::Result<Vec<_>>>()?;

        let mut snap = MetadataSnap::new(test_dir_path)?;
        snap.insert(Path::new(""), root_entries)?;
        snap.insert(Path::new("sub_folder"), sub_entries)?;

        assert!(snap.get_entry(Path::new("invalid/path")).is_none());

        {
            let file = snap.get_entry(Path::new("alpha")).unwrap();
            assert!(file.is_file());
            assert!(!file.is_dir());
        }

        {
            let dir = snap.get_entry(Path::new("sub_folder")).unwrap();
            assert!(!dir.is_file());
            assert!(dir.is_dir());
        }

        {
            let file_sub_folder = snap.get_entry(Path::new("sub_folder/beta")).unwrap();
            assert!(file_sub_folder.is_file());
            assert!(!file_sub_folder.is_dir());
        }

        {
            let symlink = snap.get_entry(Path::new("some_link")).unwrap();
            assert!(!symlink.is_file());
            assert!(!symlink.is_dir());
            assert_eq!(
                symlink.specific,
                Some(Specific::Symlink(String::from("target")))
            );
        }

        Ok(())
    }

    #[test]
    fn test_get_metadata_snap_path() {
        let cfg = Arc::new(load_ut_cfg().unwrap());
        assert_eq!(
            get_metadata_snap_path(&cfg, Path::new("/data/folder")),
            cfg.local_metadata_snap_path_user
                .join("data-folder.pb.bin.zst")
        );
    }
}
