//! File handling

#![allow(unsafe_code)]

use std::os::fd::RawFd;

use prost_types::Timestamp;

pub trait DirRoot<F>
where
    Self: Sized,
{
    /// Create new instance, by opening root directory of a tree
    ///
    /// # Errors
    /// - Returns error if the path cannot be opened
    fn new(path: &str) -> anyhow::Result<Self>;

    /// Open file for read access (`openat`)
    ///
    /// # Errors
    /// - Returns error if the file cannot be opened
    fn open(&self, rel_path: &str) -> anyhow::Result<F>;

    /// Create directory (`mkdirat`)
    ///
    /// # Errors
    /// - Returns error if the directory cannot be created
    fn mkdir(&self, rel_path: &str) -> anyhow::Result<()>;

    /// Create temporary file (`openat(O_TMPFILE)`)
    ///
    /// Process to create a file:
    /// - `let f = d.create_tmp()`
    /// - `f.write()`
    /// - `f.chown()`
    /// - `f.chmod()`
    /// - `f.set_mtime()`
    /// - `d.commit_tmp(f)` to create the file entry in the directory
    ///
    /// # Errors
    /// - Returns error if the temporary file cannot be created
    fn create_tmp(&self, rel_parent: &str, size: u64) -> anyhow::Result<F>;

    /// Commit temporary file (`linkat`)
    ///
    /// # Errors
    /// - Returns error if the file cannot be committed
    fn commit_tmp(&self, rel_path: &str, f: F) -> anyhow::Result<()>;

    /// Create a symbolic link (`symlinkat`)
    ///
    /// # Errors
    /// - Returns error if the symlink cannot be created
    fn symlink(&self, rel_path: &str, target: &str) -> anyhow::Result<()>;

    /// Delete a file or socket relative to an opened directory (`unlinkat`)
    ///
    /// # Errors
    /// - Returns error if the file cannot be removed
    fn remove_file(&self, rel_path: &str) -> anyhow::Result<()>;

    /// Delete an empty directory or socket relative to an opened directory (`unlinkat`)
    ///
    /// # Errors
    /// - Returns error if the directory cannot be removed
    fn remove_dir(&self, rel_path: &str) -> anyhow::Result<()>;

    /// Rename a file (`renameat`)
    ///
    /// # Errors
    /// - Returns error if the file cannot be renamed
    fn rename(&self, rel_oldpath: &str, rel_newpath: &str) -> anyhow::Result<()>;

    /// Close file
    ///
    /// # Errors
    /// - Returns error if the directory cannot be closed
    fn close(&mut self) -> anyhow::Result<()>;
}

pub trait MyFile
where
    Self: Sized,
{
    /// Create new instance, by wrapping a fd
    fn new(fd: RawFd) -> Self;

    /// Get file descriptor
    fn fd(&self) -> RawFd;

    /// Set owner and group of a file
    ///
    /// # Errors
    /// - Returns error if the operation fails
    fn chown(&self, owner: u32, group: u32) -> anyhow::Result<()>;

    /// Set permissions of a file
    ///   
    /// # Errors
    /// - Returns error if the operation fails
    fn chmod(&self, permissions: u32) -> anyhow::Result<()>;

    /// Set modification time of the file (`utimensat`)
    ///     
    /// # Errors
    /// - Returns error if the operation fails
    fn set_mtime(&self, ts: &Timestamp) -> anyhow::Result<()>;

    /// Write data to a file
    ///
    /// # Errors
    /// - Returns error if the operation fails
    fn write(&self, data: &[u8]) -> anyhow::Result<()>;

    /// Read data from a file, report the number of bytes read
    ///
    /// # Errors
    /// - Returns error if the operation fails
    fn read(&self, data: &mut [u8]) -> anyhow::Result<u64>;

    /// Close file
    ///
    /// # Errors
    /// - Returns error if the file cannot be closed
    fn close(&mut self) -> anyhow::Result<()>;
}
