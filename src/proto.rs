//! Protobuf related code

#![allow(
    clippy::enum_variant_names,
    clippy::large_enum_variant,
    clippy::pedantic
)]

use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};

// generated code from proto files
include!(concat!(env!("OUT_DIR"), "/mod.rs"));

pub use fs::{MyDirContent, MyDirEntry};

/// Null value for google.protobuf.NullValue fields
pub const PROTO_NULL_VALUE: i32 = 0;

/// Filetype
pub type MyFileType = libc::mode_t;
pub const MY_FILE_TYPE_FIFO: MyFileType = libc::S_IFIFO;
pub const MY_FILE_TYPE_CHARACTER: MyFileType = libc::S_IFCHR;
pub const MY_FILE_TYPE_DIRECTORY: MyFileType = libc::S_IFDIR;
pub const MY_FILE_TYPE_BLOCK: MyFileType = libc::S_IFBLK;
pub const MY_FILE_TYPE_REGULAR: MyFileType = libc::S_IFREG;
pub const MY_FILE_TYPE_SYMLINK: MyFileType = libc::S_IFLNK;
pub const MY_FILE_TYPE_SOCKET: MyFileType = libc::S_IFSOCK;
/// Get file type from mode field
pub fn mode_to_filetype(mode: u32) -> MyFileType {
    mode & libc::S_IFMT
}

pub trait MyDirEntryExt
where
    Self: Sized,
{
    /// Convert standard library entry to `MyDirEntry`
    fn try_from_std_fs(value: std::fs::DirEntry) -> anyhow::Result<Self>;

    /// Get file type
    fn file_type(&self) -> MyFileType;
}

impl MyDirEntryExt for MyDirEntry {
    fn try_from_std_fs(value: std::fs::DirEntry) -> anyhow::Result<Self> {
        /// Build `DeviceData` from rdev value
        fn build_device_data(rdev: u64) -> fs::DeviceData {
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
            fs::DeviceData {
                major: major(rdev),
                minor: minor(rdev),
            }
        }

        let metadata = value.metadata()?;
        let fs_file_type = metadata.file_type();
        let specific = if fs_file_type.is_file() {
            Some(fs::my_dir_entry::Specific::Regular(fs::RegularData {
                size: metadata.size(),
                hash: Default::default(),
            }))
        } else if fs_file_type.is_dir() {
            // content is unknown at this time
            None
        } else if fs_file_type.is_block_device() || fs_file_type.is_char_device() {
            Some(fs::my_dir_entry::Specific::Device(build_device_data(
                metadata.rdev(),
            )))
        } else if fs_file_type.is_symlink() {
            Some(fs::my_dir_entry::Specific::Symlink(
                value
                    .path()
                    .read_link()?
                    .to_str()
                    .ok_or_else(|| {
                        anyhow::anyhow!("Invalid UTF-8 for symlink at '{}'", value.path().display())
                    })?
                    .into(),
            ))
        } else {
            None
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
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            mtime,
            specific,
        })
    }

    fn file_type(&self) -> MyFileType {
        mode_to_filetype(self.mode)
    }
}
