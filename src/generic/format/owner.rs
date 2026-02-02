//! Format owner & group

use std::{collections::BTreeMap, ffi::CStr};

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
}
