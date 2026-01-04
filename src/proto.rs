//! Protobuf related code

#![allow(
    clippy::enum_variant_names,
    clippy::large_enum_variant,
    clippy::pedantic
)]

use std::cmp::Ordering;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
use std::path::Path;

// generated code from proto files
include!(concat!(env!("OUT_DIR"), "/mod.rs"));

pub use common::{DeviceData, DirectoryData, MyDirEntry, RegularData, my_dir_entry::Specific};
pub use persist::MetadataSnap;

/// Null value for google.protobuf.NullValue fields
pub const PROTO_NULL_VALUE: i32 = 0;

pub trait MyDirEntryExt
where
    Self: Sized,
{
    /// Convert standard library entry to `MyDirEntry`
    fn try_from_std_fs(value: std::fs::DirEntry) -> anyhow::Result<Self>;

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

        let metadata = value.metadata()?;
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
                value
                    .path()
                    .read_link()?
                    .to_str()
                    .ok_or_else(|| {
                        anyhow::anyhow!("Invalid UTF-8 for symlink at '{}'", value.path().display())
                    })?
                    .into(),
            )
        } else if fs_file_type.is_socket() {
            Specific::Socket(PROTO_NULL_VALUE)
        } else {
            anyhow::bail!("Unsupported file type at '{}'", value.path().display());
        };
        let file_name = value
            .file_name()
            .to_str()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid UTF-8 for file name at '{}'",
                    value.path().display()
                )
            })?
            .into();
        let mtime = Some(prost_types::Timestamp {
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

    fn insert(&mut self, rel_path: &Path, mut entries: Vec<MyDirEntry>) -> anyhow::Result<()> {
        let mut dir = self;
        for walk in rel_path {
            let d = walk
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid UTF-8 path '{rel_path:?}"))?;
            let Some(Specific::Directory(dir_data)) = &mut dir.specific else {
                anyhow::bail!(
                    "inconsistent snapshot, trying to insert entries in a non-directory entry"
                );
            };
            let index = dir_data
                .content
                .binary_search_by_key(&d, MyDirEntry::sort_key)
                .map_err(|_| {
                    anyhow::anyhow!("inconsistent snapshot, intermediate directory {d} not found")
                })?;
            dir = &mut dir_data.content[index];
        }

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
