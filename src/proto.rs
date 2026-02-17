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
pub use prost_types::Timestamp;

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

    /// Take one entry from the tree
    /// In the tree, the entry is kept (so name is still valid, allowing binary_search)
    /// but specific field is taken
    fn take_entry(&mut self, rel_path: &str) -> Option<MyDirEntry>;
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

    fn take_entry(&mut self, rel_path: &str) -> Option<MyDirEntry> {
        let src_entry = self.get_entry_mut(rel_path)?;
        Some(MyDirEntry {
            file_name: src_entry.file_name.clone(),
            specific: src_entry.specific.take(),
            ..*src_entry
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
