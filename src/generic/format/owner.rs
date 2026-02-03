//! Format owner & group

use std::collections::BTreeMap;
use std::ffi::CStr;
use std::fmt::Write as _;

use crate::proto::MyDirEntry;

pub struct OwnerGroupDb {
    /// Buffer to read owner or group name
    buf: Vec<u8>,
    /// Map uid to name
    owner: BTreeMap<u32, String>,
    /// Map gid to name
    group: BTreeMap<u32, String>,
}

impl Default for OwnerGroupDb {
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    fn default() -> Self {
        #[allow(unsafe_code)]
        let buf_size = unsafe {
            // use a single buffer, suitable for getpwuid_r & getgrgid_r
            let owner_buf_size = libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) as usize;
            let group_buf_size = libc::sysconf(libc::_SC_GETGR_R_SIZE_MAX) as usize;
            owner_buf_size.max(group_buf_size)
        };
        Self {
            buf: vec![0; buf_size],
            owner: BTreeMap::new(),
            group: BTreeMap::new(),
        }
    }
}

impl OwnerGroupDb {
    /// Get owner name
    pub fn get_owner(&mut self, uid: u32) -> &str {
        self.owner
            .entry(uid)
            .or_insert_with(|| Self::get_owner_name(&mut self.buf, uid))
            .as_str()
    }

    /// Get group name
    pub fn get_group(&mut self, gid: u32) -> &str {
        self.group
            .entry(gid)
            .or_insert_with(|| Self::get_gid_name(&mut self.buf, gid))
            .as_str()
    }

    pub fn format_owner_group(&mut self, entry: &MyDirEntry) -> String {
        let mut res = String::with_capacity(15); // owner and group are usually ASCII
        let owner = self.get_owner(entry.uid);
        write!(&mut res, "{owner:>7.7}:").unwrap();
        let group = self.get_group(entry.gid);
        write!(&mut res, "{group:<7.7}").unwrap();
        res
    }

    /// Perform syscall to get owner name
    fn get_owner_name(buf: &mut [u8], uid: u32) -> String {
        #[allow(unsafe_code)]
        unsafe {
            let mut pwd: libc::passwd = std::mem::zeroed();
            let mut result: *mut libc::passwd = std::ptr::null_mut();
            let _ = libc::getpwuid_r(
                uid,
                &raw mut pwd,
                buf.as_mut_ptr().cast(),
                buf.len(),
                &raw mut result,
            );
            if result.is_null() {
                format!("{uid}")
            } else {
                CStr::from_ptr(pwd.pw_name).to_str().unwrap().into()
            }
        }
    }

    /// Perform syscall to get group name
    fn get_gid_name(buf: &mut [u8], gid: u32) -> String {
        #[allow(unsafe_code)]
        unsafe {
            let mut grp: libc::group = std::mem::zeroed();
            let mut result: *mut libc::group = std::ptr::null_mut();
            let _ = libc::getgrgid_r(
                gid,
                &raw mut grp,
                buf.as_mut_ptr().cast(),
                buf.len(),
                &raw mut result,
            );
            if result.is_null() {
                format!("{gid}")
            } else {
                CStr::from_ptr(grp.gr_name).to_str().unwrap().into()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::MyDirEntry;

    #[test]
    fn test_owner_group_db() {
        let mut db = OwnerGroupDb::default();

        // Test root (0) - exercises the syscalls
        assert_eq!(db.get_owner(0), "root");
        assert_eq!(db.get_group(0), "root");

        // Inject invalid values in cache
        db.owner.insert(14, "test_invalid".to_string());
        db.group.insert(11, "test_invalid".to_string());

        // Values can be retrieved without syscall
        assert_eq!(db.get_owner(14), "test_invalid");
        assert_eq!(db.get_group(11), "test_invalid");
    }

    #[test]
    fn test_format_owner_group() {
        let mut db = OwnerGroupDb::default();

        // Test with root user and group (from syscall)
        let entry = MyDirEntry {
            file_name: "test".to_string(),
            permissions: 0,
            uid: 0, // root
            gid: 0, // root
            mtime: None,
            specific: None,
        };
        assert_eq!(db.format_owner_group(&entry), "   root:root   ");

        // Test with injected values
        db.owner.insert(1001, "user1".to_string());
        db.group.insert(1002, "group2".to_string());
        let entry2 = MyDirEntry {
            file_name: "test".to_string(),
            permissions: 0,
            uid: 1001,
            gid: 1002,
            mtime: None,
            specific: None,
        };
        assert_eq!(db.format_owner_group(&entry2), "  user1:group2 ");

        // Test with long names that will be truncated
        db.owner.insert(1003, "longusername".to_string());
        db.group.insert(1004, "longgroupname".to_string());
        let entry3 = MyDirEntry {
            file_name: "test".to_string(),
            permissions: 0,
            uid: 1003,
            gid: 1004,
            mtime: None,
            specific: None,
        };
        assert_eq!(db.format_owner_group(&entry3), "longuse:longgro");
    }
}
