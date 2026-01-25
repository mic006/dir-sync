//! Protobuf related code

#![allow(
    clippy::enum_variant_names,
    clippy::large_enum_variant,
    clippy::pedantic
)]

use std::cmp::Ordering;
use std::path::{Path, PathBuf};

// generated code from proto files
include!(concat!(env!("OUT_DIR"), "/mod.rs"));

pub use action::{action_req::Req as ActionReq, action_rsp::Rsp as ActionRsp};
pub use common::{DeviceData, DirectoryData, MyDirEntry, RegularData, my_dir_entry::Specific};
pub use persist::MetadataSnap;
pub use prost_types::Timestamp;

use crate::config::ConfigRef;
use crate::generic::fs::systemd_escape_path;

/// Null value for google.protobuf.NullValue fields
pub const PROTO_NULL_VALUE: i32 = 0;

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

    /// Get one entry in the tree
    fn get_entry(&self, rel_path: &str) -> Option<&MyDirEntry>;

    /// Get one entry in the tree
    fn get_entry_mut(&mut self, rel_path: &str) -> Option<&mut MyDirEntry>;

    /// Insert content in the tree
    fn insert(
        &mut self,
        // relative path of directory to root directory, "." if root
        rel_path: &str,
        // directory content, unsorted
        entries: Vec<MyDirEntry>,
    ) -> anyhow::Result<()>;
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

    fn insert(&mut self, rel_path: &str, mut entries: Vec<MyDirEntry>) -> anyhow::Result<()> {
        let dir = self.get_entry_mut(rel_path).ok_or_else(|| {
            anyhow::anyhow!("inconsistent snapshot, directory '{rel_path}' not found",)
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
pub fn get_metadata_snap_path(cfg: &ConfigRef, input_path: &str) -> PathBuf {
    let mut path = cfg
        .local_metadata_snap_path_user
        .join(systemd_escape_path(input_path));
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
    fn test_get_metadata_snap_path() {
        let cfg = Arc::new(load_ut_cfg().unwrap());
        assert_eq!(
            get_metadata_snap_path(&cfg, "/data/folder"),
            cfg.local_metadata_snap_path_user
                .join("data-folder.pb.bin.zst")
        );
    }
}
