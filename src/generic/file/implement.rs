//! File handling

#![allow(unsafe_code)]

use std::{ffi::CString, marker::PhantomData, os::fd::RawFd};

use prost_types::Timestamp;

use super::api::{DirRoot, MyFile};

pub struct DirRootImpl<F: MyFile> {
    fd: RawFd,
    _phantom: PhantomData<F>,
}

impl<F: MyFile> DirRoot<F> for DirRootImpl<F> {
    fn new(path: &str) -> anyhow::Result<Self> {
        unsafe {
            let c_path = CString::new(path)?;
            let fd = libc::open(
                c_path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_PATH | libc::O_CLOEXEC,
            );
            anyhow::ensure!(
                fd >= 0,
                "DirRootImpl::open({path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(Self {
                fd,
                _phantom: PhantomData,
            })
        }
    }

    fn open(&self, rel_path: &str) -> anyhow::Result<F> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let fd = libc::openat(
                self.fd,
                c_rel_path.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC,
            );
            anyhow::ensure!(
                fd >= 0,
                "DirRootImpl::open({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(F::new(fd))
        }
    }

    fn mkdir(&self, rel_path: &str) -> anyhow::Result<()> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let res = libc::mkdirat(
                self.fd,
                c_rel_path.as_ptr(),
                libc::S_IRUSR | libc::S_IWUSR | libc::S_IXUSR,
            );
            anyhow::ensure!(
                res == 0,
                "DirRootImpl::mkdir({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    fn create_tmp(&self, rel_parent: &str, size: u64) -> anyhow::Result<F> {
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
                "DirRootImpl::create_tmp({rel_parent}) creation failed: {}",
                std::io::Error::last_os_error()
            );

            // reserve data space
            let res = libc::fallocate(fd, 0, 0, size.cast_signed());
            anyhow::ensure!(
                res == 0,
                "DirRootImpl::create_tmp({rel_parent}) allocation failed: {}",
                std::io::Error::last_os_error()
            );

            Ok(F::new(fd))
        }
    }

    fn commit_tmp(&self, rel_path: &str, mut f: F) -> anyhow::Result<()> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let res = libc::linkat(
                f.fd(),
                c"".as_ptr(),
                self.fd,
                c_rel_path.as_ptr(),
                libc::AT_EMPTY_PATH,
            );
            anyhow::ensure!(
                res == 0,
                "DirRootImpl::commit_tmp({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            f.close()?;
            Ok(())
        }
    }

    fn symlink(&self, rel_path: &str, target: &str) -> anyhow::Result<()> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let c_target = CString::new(target)?;
            let res = libc::symlinkat(c_target.as_ptr(), self.fd, c_rel_path.as_ptr());
            anyhow::ensure!(
                res == 0,
                "DirRootImpl::symlink({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    fn remove_file(&self, rel_path: &str) -> anyhow::Result<()> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let res = libc::unlinkat(self.fd, c_rel_path.as_ptr(), 0);
            anyhow::ensure!(
                res == 0,
                "DirRootImpl::remove_file({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    fn remove_dir(&self, rel_path: &str) -> anyhow::Result<()> {
        unsafe {
            let c_rel_path = CString::new(rel_path)?;
            let res = libc::unlinkat(self.fd, c_rel_path.as_ptr(), libc::AT_REMOVEDIR);
            anyhow::ensure!(
                res == 0,
                "DirRootImpl::remove_dir({rel_path}) failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    fn rename(&self, rel_oldpath: &str, rel_newpath: &str) -> anyhow::Result<()> {
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
                "DirRootImpl::rename({rel_oldpath}, {rel_newpath}) failed: {}",
                std::io::Error::last_os_error()
            );
            // TODO: fsync of rel_newpath parent dir
            Ok(())
        }
    }

    fn close(&mut self) -> anyhow::Result<()> {
        DirRootImpl::close(self)
    }
}
impl<F: MyFile> DirRootImpl<F> {
    fn close(&mut self) -> anyhow::Result<()> {
        unsafe {
            if self.fd >= 0 {
                let res = libc::close(self.fd);
                self.fd = -1;
                anyhow::ensure!(
                    res == 0,
                    "DirRootImpl::close() failed: {}",
                    std::io::Error::last_os_error()
                );
            }
            Ok(())
        }
    }
}
impl<F: MyFile> Drop for DirRootImpl<F> {
    fn drop(&mut self) {
        drop(self.close());
    }
}

/// File handling adapted to `dir-sync` needs
pub struct MyFileImpl {
    fd: RawFd,
}

impl MyFile for MyFileImpl {
    fn new(fd: RawFd) -> Self {
        Self { fd }
    }

    fn fd(&self) -> RawFd {
        self.fd
    }

    fn chown(&self, owner: u32, group: u32) -> anyhow::Result<()> {
        unsafe {
            let res = libc::fchown(self.fd, owner, group);
            anyhow::ensure!(
                res == 0,
                "MyFileImpl::chown() failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    fn chmod(&self, permissions: u32) -> anyhow::Result<()> {
        unsafe {
            let res = libc::fchmod(self.fd, permissions);
            anyhow::ensure!(
                res == 0,
                "MyFileImpl::chmod() failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    fn set_mtime(&self, ts: &Timestamp) -> anyhow::Result<()> {
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
                "MyFileImpl::set_mtime() failed: {}",
                std::io::Error::last_os_error()
            );
            Ok(())
        }
    }

    fn write(&self, data: &[u8]) -> anyhow::Result<()> {
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
                    "MyFileImpl::write() failed: {}",
                    std::io::Error::last_os_error()
                );
                written += res.cast_unsigned();
            }
            Ok(())
        }
    }

    fn read(&self, data: &mut [u8]) -> anyhow::Result<u64> {
        unsafe {
            let res = libc::read(self.fd, data.as_mut_ptr().cast(), data.len());
            anyhow::ensure!(
                res >= 0,
                "MyFileImpl::read() failed: {}",
                std::io::Error::last_os_error()
            );
            #[allow(clippy::cast_sign_loss)]
            Ok(res as u64)
        }
    }

    fn close(&mut self) -> anyhow::Result<()> {
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
impl Drop for MyFileImpl {
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
    fn test_dir_root_impl_new() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_root = DirRootImpl::<MyFileImpl>::new(temp_dir.path().to_str().unwrap())?;
        assert!(dir_root.fd >= 0);
        Ok(())
    }

    #[test]
    fn test_dir_root_impl_new_invalid_path() {
        let result: anyhow::Result<DirRootImpl<MyFileImpl>> =
            DirRootImpl::new("/nonexistent/path/to/directory");
        assert!(result.is_err());
    }

    #[test]
    fn test_my_file_impl_write_and_read() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_root = DirRootImpl::<MyFileImpl>::new(temp_dir.path().to_str().unwrap())?;

        // Create a temporary file
        let tmp_file = dir_root.create_tmp(".", 4096)?;

        // Write data to the file
        let data = b"Hello, World!";
        tmp_file.write(data)?;

        // Commit the temporary file
        dir_root.commit_tmp("test_file.txt", tmp_file)?;

        // Verify the file was created and has the correct content
        let content = fs::read(temp_dir.path().join("test_file.txt"))?;
        // The file includes the written data followed by allocated zeros from fallocate
        assert!(content.starts_with(data));
        assert_eq!(content.len(), 4096);

        Ok(())
    }

    #[test]
    fn test_my_file_impl_chmod() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_root = DirRootImpl::<MyFileImpl>::new(temp_dir.path().to_str().unwrap())?;

        // Create and commit a file
        let tmp_file = dir_root.create_tmp(".", 1024)?;
        tmp_file.chmod(0o644)?;
        dir_root.commit_tmp("test_chmod.txt", tmp_file)?;

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
    fn test_dir_root_impl_symlink() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_root = DirRootImpl::<MyFileImpl>::new(temp_dir.path().to_str().unwrap())?;

        // Create a symlink to the target file
        dir_root.symlink("link_to_target.txt", "target_file.txt")?;

        // Verify the symlink exists and points to the correct target
        let link_path = temp_dir.path().join("link_to_target.txt");
        let target = fs::read_link(&link_path)?;
        assert_eq!(target.to_str().unwrap(), "target_file.txt");

        Ok(())
    }

    #[test]
    fn test_dir_root_impl_remove_file() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_root = DirRootImpl::<MyFileImpl>::new(temp_dir.path().to_str().unwrap())?;

        // Create a file
        let tmp_file = dir_root.create_tmp(".", 1024)?;
        dir_root.commit_tmp("file_to_remove.txt", tmp_file)?;

        // Verify the file exists
        assert!(temp_dir.path().join("file_to_remove.txt").exists());

        // Remove the file
        dir_root.remove_file("file_to_remove.txt")?;

        // Verify the file is gone
        assert!(!temp_dir.path().join("file_to_remove.txt").exists());

        Ok(())
    }

    #[test]
    fn test_dir_root_impl_remove_dir() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_root = DirRootImpl::<MyFileImpl>::new(temp_dir.path().to_str().unwrap())?;

        // Create a directory
        let subdir_path = temp_dir.path().join("subdir");
        dir_root.mkdir("subdir")?;

        // Verify the directory exists
        assert!(subdir_path.exists());

        // Remove the directory
        dir_root.remove_dir("subdir")?;

        // Verify the directory is gone
        assert!(!subdir_path.exists());

        Ok(())
    }

    #[test]
    fn test_dir_root_impl_rename() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_root = DirRootImpl::<MyFileImpl>::new(temp_dir.path().to_str().unwrap())?;

        // Create a file
        let tmp_file = dir_root.create_tmp(".", 1024)?;
        dir_root.commit_tmp("original_name.txt", tmp_file)?;

        // Verify the file exists
        assert!(temp_dir.path().join("original_name.txt").exists());

        // Rename the file
        dir_root.rename("original_name.txt", "new_name.txt")?;

        // Verify the file is renamed
        assert!(!temp_dir.path().join("original_name.txt").exists());
        assert!(temp_dir.path().join("new_name.txt").exists());

        Ok(())
    }

    #[test]
    fn test_my_file_impl_multiple_writes() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_root = DirRootImpl::<MyFileImpl>::new(temp_dir.path().to_str().unwrap())?;

        // Create a temporary file
        let tmp_file = dir_root.create_tmp(".", 4096)?;

        // Write data in multiple chunks
        let chunk1 = b"First chunk ";
        let chunk2 = b"Second chunk ";
        let chunk3 = b"Third chunk";

        tmp_file.write(chunk1)?;
        tmp_file.write(chunk2)?;
        tmp_file.write(chunk3)?;
        dir_root.commit_tmp("multi_write.txt", tmp_file)?;

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
    fn test_my_file_impl_set_mtime() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let dir_root = DirRootImpl::<MyFileImpl>::new(temp_dir.path().to_str().unwrap())?;

        // Create a temporary file
        let tmp_file = dir_root.create_tmp(".", 1024)?;

        // Set modification time
        let ts = Timestamp {
            seconds: 1_609_459_200, // 2021-01-01 00:00:00 UTC
            nanos: 0,
        };
        tmp_file.set_mtime(&ts)?;

        dir_root.commit_tmp("mtime_test.txt", tmp_file)?;

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
