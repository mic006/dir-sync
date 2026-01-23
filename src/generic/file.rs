//! File handling

#![allow(unsafe_code)]

use std::ffi::CString;
use std::os::fd::RawFd;

use prost_types::Timestamp;

/// Directory tree operations
pub struct FsTree {
    fd: RawFd,
}

impl FsTree {
    /// Create new instance, by opening root directory of a tree
    ///
    /// # Errors
    /// - Returns error if the path cannot be opened
    pub fn new(path: &str) -> anyhow::Result<Self> {
        unsafe {
            let c_path = CString::new(path)?;
            let fd = libc::open(
                c_path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_PATH | libc::O_CLOEXEC,
            );
            anyhow::ensure!(
                fd >= 0,
                "FsTree::new({path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(Self { fd })
        }
    }

    /// Open file for read access (`openat`)
    ///
    /// # Errors
    /// - Returns error if the file cannot be opened
    pub fn open(&self, rel_path: &str) -> anyhow::Result<FsFile> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let fd = libc::openat(
                self.fd,
                c_rel_path.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC,
            );
            anyhow::ensure!(
                fd >= 0,
                "FsTree::open({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(FsFile { fd })
        }
    }

    /// Create directory (`mkdirat`)
    ///
    /// # Errors
    /// - Returns error if the directory cannot be created
    pub fn mkdir(&self, rel_path: &str) -> anyhow::Result<()> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let res = libc::mkdirat(
                self.fd,
                c_rel_path.as_ptr(),
                libc::S_IRUSR | libc::S_IWUSR | libc::S_IXUSR,
            );
            anyhow::ensure!(
                res == 0,
                "FsTree::mkdir({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

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
    pub fn create_tmp(&self, rel_parent: &str, size: u64) -> anyhow::Result<FsFile> {
        unsafe {
            let c_rel_parent = CString::new(rel_parent)?;
            // create temp file
            let fd = libc::openat(
                self.fd,
                c_rel_parent.as_ptr(),
                libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC,
                libc::S_IRUSR | libc::S_IWUSR,
            );
            anyhow::ensure!(
                fd >= 0,
                "FsTree::create_tmp({rel_parent}) creation failed: {}",
                std::io::Error::last_os_error()
            );

            // reserve data space
            let res = libc::fallocate(fd, 0, 0, size.cast_signed());
            anyhow::ensure!(
                res == 0,
                "FsTree::create_tmp({rel_parent}) allocation failed: {}",
                std::io::Error::last_os_error()
            );

            Ok(FsFile { fd })
        }
    }

    /// Commit temporary file (`linkat`)
    ///
    /// # Errors
    /// - Returns error if the file cannot be committed
    pub fn commit_tmp(&self, rel_path: &str, mut f: FsFile) -> anyhow::Result<()> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let res = libc::linkat(
                f.fd,
                c"".as_ptr(),
                self.fd,
                c_rel_path.as_ptr(),
                libc::AT_EMPTY_PATH,
            );
            anyhow::ensure!(
                res == 0,
                "FsTree::commit_tmp({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            f.close()?;
            Ok(())
        }
    }

    /// Create a symbolic link (`symlinkat`)
    ///
    /// # Errors
    /// - Returns error if the symlink cannot be created
    pub fn symlink(&self, rel_path: &str, target: &str) -> anyhow::Result<()> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let c_target = CString::new(target)?;
            let res = libc::symlinkat(c_target.as_ptr(), self.fd, c_rel_path.as_ptr());
            anyhow::ensure!(
                res == 0,
                "FsTree::symlink({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    /// Delete a file or socket relative to an opened directory (`unlinkat`)
    ///
    /// # Errors
    /// - Returns error if the file cannot be removed
    pub fn remove_file(&self, rel_path: &str) -> anyhow::Result<()> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let res = libc::unlinkat(self.fd, c_rel_path.as_ptr(), 0);
            anyhow::ensure!(
                res == 0,
                "FsTree::remove_file({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    /// Delete an empty directory or socket relative to an opened directory (`unlinkat`)
    ///
    /// # Errors
    /// - Returns error if the directory cannot be removed
    pub fn remove_dir(&self, rel_path: &str) -> anyhow::Result<()> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let res = libc::unlinkat(self.fd, c_rel_path.as_ptr(), libc::AT_REMOVEDIR);
            anyhow::ensure!(
                res == 0,
                "FsTree::remove_dir({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    /// Rename a file (`renameat`)
    ///
    /// # Errors
    /// - Returns error if the file cannot be renamed
    pub fn rename(&self, rel_oldpath: &str, rel_newpath: &str) -> anyhow::Result<()> {
        unsafe {
            let c_rel_oldpath = CString::new(rel_oldpath)?;
            let c_rel_newpath = CString::new(rel_newpath)?;
            let res = libc::renameat(
                self.fd,
                c_rel_oldpath.as_ptr(),
                self.fd,
                c_rel_newpath.as_ptr(),
            );
            anyhow::ensure!(
                res == 0,
                "FsTree::rename({rel_oldpath}, {rel_newpath}) failed: {}",
                std::io::Error::last_os_error()
            );
            // TODO: fsync of rel_newpath parent dir
            Ok(())
        }
    }

    /// Close file
    ///
    /// # Errors
    /// - Returns error if the directory cannot be closed
    pub fn close(&mut self) -> anyhow::Result<()> {
        self.close_internal()
    }

    fn close_internal(&mut self) -> anyhow::Result<()> {
        unsafe {
            if self.fd >= 0 {
                let res = libc::close(self.fd);
                self.fd = -1;
                anyhow::ensure!(
                    res == 0,
                    "FsTree::close() failed: {}",
                    std::io::Error::last_os_error()
                );
            }
            Ok(())
        }
    }
}

impl Drop for FsTree {
    fn drop(&mut self) {
        drop(self.close_internal());
    }
}

/// File descriptor wrapper
pub struct FsFile {
    fd: RawFd,
}

impl FsFile {
    /// Change owner of the file (`fchown`)
    ///
    /// # Errors
    /// - Returns error if the ownership cannot be changed
    pub fn chown(&self, owner: u32, group: u32) -> anyhow::Result<()> {
        unsafe {
            let res = libc::fchown(self.fd, owner, group);
            anyhow::ensure!(
                res == 0,
                "FsFile::chown() failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    /// Change mode of the file (`fchmod`)
    ///
    /// # Errors
    /// - Returns error if the mode cannot be changed
    pub fn chmod(&self, permissions: u32) -> anyhow::Result<()> {
        unsafe {
            let res = libc::fchmod(self.fd, permissions);
            anyhow::ensure!(
                res == 0,
                "FsFile::chmod() failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    /// Set modification time (`futimens`)
    ///
    /// # Errors
    /// - Returns error if the time cannot be set
    pub fn set_mtime(&self, ts: &Timestamp) -> anyhow::Result<()> {
        unsafe {
            let times = [
                libc::timespec {
                    // atime
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_OMIT,
                },
                libc::timespec {
                    // mtime
                    tv_sec: ts.seconds,
                    tv_nsec: ts.nanos.into(),
                },
            ];
            let res = libc::futimens(self.fd, times.as_ptr());
            anyhow::ensure!(
                res == 0,
                "FsFile::set_mtime() failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    /// Write data to the file (`write`)
    ///
    /// # Errors
    /// - Returns error if the data cannot be written
    pub fn write(&self, data: &[u8]) -> anyhow::Result<()> {
        unsafe {
            let mut written = 0;
            while written < data.len() {
                let res = libc::write(
                    self.fd,
                    data[written..].as_ptr().cast(),
                    data.len() - written,
                );
                anyhow::ensure!(
                    res >= 0,
                    "FsFile::write() failed: {}",
                    std::io::Error::last_os_error()
                );
                written += res.cast_unsigned();
            }
            Ok(())
        }
    }

    /// Read data from the file (`read`)
    ///
    /// # Errors
    /// - Returns error if the data cannot be read
    pub fn read(&self, data: &mut [u8]) -> anyhow::Result<u64> {
        unsafe {
            let res = libc::read(self.fd, data.as_mut_ptr().cast(), data.len());
            anyhow::ensure!(
                res >= 0,
                "FsFile::read() failed: {}",
                std::io::Error::last_os_error()
            );
            #[allow(clippy::cast_sign_loss)]
            Ok(res as u64)
        }
    }

    /// Close the file (`close`)
    ///
    /// # Errors
    /// - Returns error if the file cannot be closed
    pub fn close(&mut self) -> anyhow::Result<()> {
        unsafe {
            if self.fd >= 0 {
                let res = libc::fsync(self.fd);
                anyhow::ensure!(
                    res == 0,
                    "MyFileImpl::fsync() failed: {}",
                    std::io::Error::last_os_error()
                );
                let res = libc::close(self.fd);
                self.fd = -1;
                anyhow::ensure!(
                    res == 0,
                    "MyFileImpl::close() failed: {}",
                    std::io::Error::last_os_error()
                );
            }
            Ok(())
        }
    }
}

impl Drop for FsFile {
    fn drop(&mut self) {
        drop(self.close());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_fs_tree_impl_new() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let fs_tree = FsTree::new(temp_dir.path().to_str().unwrap())?;
        assert!(fs_tree.fd >= 0);
        Ok(())
    }

    #[test]
    fn test_fs_tree_impl_new_invalid_path() {
        let result: anyhow::Result<FsTree> = FsTree::new("/nonexistent/path/to/directory");
        assert!(result.is_err());
    }

    #[test]
    fn test_fs_file_impl_write_and_read() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let fs_tree = FsTree::new(temp_dir.path().to_str().unwrap())?;

        // Create a temporary file
        let tmp_file = fs_tree.create_tmp(".", 4096)?;

        // Write data to the file
        let data = b"Hello, World!";
        tmp_file.write(data)?;

        // Commit the temporary file
        fs_tree.commit_tmp("test_file.txt", tmp_file)?;

        // Verify the file was created and has the correct content
        let content = fs::read(temp_dir.path().join("test_file.txt"))?;
        // The file includes the written data followed by allocated zeros from fallocate
        assert!(content.starts_with(data));
        assert_eq!(content.len(), 4096);

        Ok(())
    }

    #[test]
    fn test_fs_file_impl_chmod() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let fs_tree = FsTree::new(temp_dir.path().to_str().unwrap())?;

        // Create and commit a file
        let tmp_file = fs_tree.create_tmp(".", 1024)?;
        tmp_file.chmod(0o644)?;
        fs_tree.commit_tmp("test_chmod.txt", tmp_file)?;

        // Verify permissions
        let metadata = fs::metadata(temp_dir.path().join("test_chmod.txt"))?;
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = metadata.permissions().mode();
            assert_eq!(mode & 0o777, 0o644);
        }

        Ok(())
    }

    #[test]
    fn test_fs_tree_impl_symlink() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let fs_tree = FsTree::new(temp_dir.path().to_str().unwrap())?;

        // Create a symlink to the target file
        fs_tree.symlink("link_to_target.txt", "target_file.txt")?;

        // Verify the symlink exists and points to the correct target
        let link_path = temp_dir.path().join("link_to_target.txt");
        let target = fs::read_link(&link_path)?;
        assert_eq!(target.to_str().unwrap(), "target_file.txt");

        Ok(())
    }

    #[test]
    fn test_fs_tree_impl_remove_file() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let fs_tree = FsTree::new(temp_dir.path().to_str().unwrap())?;

        // Create a file
        let tmp_file = fs_tree.create_tmp(".", 1024)?;
        fs_tree.commit_tmp("file_to_remove.txt", tmp_file)?;

        // Verify the file exists
        assert!(temp_dir.path().join("file_to_remove.txt").exists());

        // Remove the file
        fs_tree.remove_file("file_to_remove.txt")?;

        // Verify the file is gone
        assert!(!temp_dir.path().join("file_to_remove.txt").exists());

        Ok(())
    }

    #[test]
    fn test_fs_tree_impl_mkdir_remove_dir() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let fs_tree = FsTree::new(temp_dir.path().to_str().unwrap())?;

        // Create a directory
        let subdir_path = temp_dir.path().join("subdir");
        fs_tree.mkdir("subdir")?;

        // Verify the directory exists
        assert!(subdir_path.exists());

        // Remove the directory
        fs_tree.remove_dir("subdir")?;

        // Verify the directory is gone
        assert!(!subdir_path.exists());

        Ok(())
    }

    #[test]
    fn test_fs_tree_impl_rename() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let fs_tree = FsTree::new(temp_dir.path().to_str().unwrap())?;

        // Create a file
        let tmp_file = fs_tree.create_tmp(".", 1024)?;
        fs_tree.commit_tmp("original_name.txt", tmp_file)?;

        // Verify the file exists
        assert!(temp_dir.path().join("original_name.txt").exists());

        // Rename the file
        fs_tree.rename("original_name.txt", "new_name.txt")?;

        // Verify the file is renamed
        assert!(!temp_dir.path().join("original_name.txt").exists());
        assert!(temp_dir.path().join("new_name.txt").exists());

        Ok(())
    }

    #[test]
    fn test_fs_file_impl_multiple_writes() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let fs_tree = FsTree::new(temp_dir.path().to_str().unwrap())?;

        // Create a temporary file
        let tmp_file = fs_tree.create_tmp(".", 4096)?;

        // Write data in multiple chunks
        let chunk1 = b"First chunk ";
        let chunk2 = b"Second chunk ";
        let chunk3 = b"Third chunk";

        tmp_file.write(chunk1)?;
        tmp_file.write(chunk2)?;
        tmp_file.write(chunk3)?;
        fs_tree.commit_tmp("multi_write.txt", tmp_file)?;

        // Verify the file content - should contain the written data + allocated zeros
        let content = fs::read(temp_dir.path().join("multi_write.txt"))?;
        let mut expected = Vec::new();
        expected.extend_from_slice(chunk1);
        expected.extend_from_slice(chunk2);
        expected.extend_from_slice(chunk3);

        assert!(content.starts_with(&expected));
        assert_eq!(content.len(), 4096); // Size as allocated

        Ok(())
    }

    #[test]
    fn test_fs_file_impl_set_mtime() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let fs_tree = FsTree::new(temp_dir.path().to_str().unwrap())?;

        // Create a temporary file
        let tmp_file = fs_tree.create_tmp(".", 1024)?;

        // Set modification time
        let ts = Timestamp {
            seconds: 1_609_459_200, // 2021-01-01 00:00:00 UTC
            nanos: 0,
        };
        tmp_file.set_mtime(&ts)?;

        fs_tree.commit_tmp("mtime_test.txt", tmp_file)?;

        // Verify modification time
        let metadata = fs::metadata(temp_dir.path().join("mtime_test.txt"))?;
        let mtime = metadata.modified()?;

        {
            use std::time::SystemTime;
            let duration = mtime
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            assert_eq!(duration.as_secs().cast_signed(), ts.seconds);
        }

        Ok(())
    }
}
