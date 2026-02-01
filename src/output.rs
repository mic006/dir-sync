//! Output of `MetadataSnap`

use std::io::Write as _;

use prost_types::Timestamp;

use crate::generic::libc::strftime_local;
use crate::proto::{MetadataSnap, MyDirEntry, Specific};

/// Output `MetadataSnap` to stdout
pub fn output(snap: &MetadataSnap) {
    let _ignored = output_exit_on_error(snap);
}

/// Get timestamp in human format
fn get_ts(ts: Option<&Timestamp>) -> String {
    ts.map_or_else(|| String::from("N/A"), strftime_local)
}

fn output_exit_on_error(snap: &MetadataSnap) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout();

    writeln!(stdout, "ts: {}", get_ts(snap.ts.as_ref()))?;

    writeln!(stdout, "path: {}", snap.path)?;

    if snap.last_syncs.is_empty() {
        writeln!(stdout, "last_syncs: []")?;
    } else {
        writeln!(stdout, "last_syncs:")?;
        for sync_status in &snap.last_syncs {
            writeln!(
                stdout,
                "  - {}  {}",
                get_ts(sync_status.ts.as_ref()),
                sync_status.sync_path
            )?;
        }
    }

    if let Some(_root) = &snap.root {
        todo!();
    } else {
        writeln!(stdout, "root: None")?;
    }

    Ok(())
}

/// Get file type and permissions
fn file_type_and_permissions(entry: &MyDirEntry) -> String {
    let mut res = vec![0u8; 10];
    //let mut res = String::with_capacity(10);
    res[0] = match &entry.specific {
        Some(Specific::Fifo(_)) => b'|',
        Some(Specific::Character(_)) => b'c',
        Some(Specific::Directory(_)) => b'd',
        Some(Specific::Block(_)) => b'b',
        Some(Specific::Regular(_)) => b'.',
        Some(Specific::Symlink(_)) => b'l',
        Some(Specific::Socket(_)) => b's',
        None => todo!(),
    };

    // standard permissions
    let perm = entry.permissions;
    for (idx, p) in [
        (1, (perm & 0o700) >> 6), // owner
        (4, (perm & 0o070) >> 3), // group
        (7, perm & 0o007),        // other
    ] {
        res[idx] = if (p & 0o4) != 0 { b'r' } else { b'-' };
        res[idx + 1] = if (p & 0o2) != 0 { b'w' } else { b'-' };
        res[idx + 2] = if (p & 0o1) != 0 { b'x' } else { b'-' };
    }

    // special bits
    if (perm & libc::S_ISUID) != 0 {
        // set-user-ID bit
        res[3] = if (perm & libc::S_IXUSR) != 0 {
            b's'
        } else {
            b'S'
        };
    }
    if (perm & libc::S_ISGID) != 0 {
        // set-group-ID bit
        res[6] = if (perm & libc::S_IXGRP) != 0 {
            b's'
        } else {
            b'S'
        };
    }
    if (perm & libc::S_ISVTX) != 0 {
        // sticky bit
        res[9] = if (perm & libc::S_IXOTH) != 0 {
            b't'
        } else {
            b'T'
        };
    }

    #[allow(unsafe_code)]
    unsafe {
        String::from_utf8_unchecked(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{DeviceData, DirectoryData, PROTO_NULL_VALUE, RegularData};

    #[test]
    fn test_file_type_and_permissions() {
        // Helper to create a dummy entry
        let create_entry = |specific: Specific, permissions: u32| -> MyDirEntry {
            MyDirEntry {
                file_name: "test".to_string(),
                permissions,
                uid: 1000,
                gid: 1000,
                mtime: None,
                specific: Some(specific),
            }
        };

        // 1. Regular file
        let entry = create_entry(
            Specific::Regular(RegularData {
                size: 10,
                hash: vec![],
            }),
            0o644,
        );
        assert_eq!(file_type_and_permissions(&entry), ".rw-r--r--");

        // 2. Directory
        let entry = create_entry(
            Specific::Directory(DirectoryData { content: vec![] }),
            0o755,
        );
        assert_eq!(file_type_and_permissions(&entry), "drwxr-xr-x");

        // 3. Symlink
        let entry = create_entry(Specific::Symlink("target".into()), 0o777);
        assert_eq!(file_type_and_permissions(&entry), "lrwxrwxrwx");

        // 4. Block device
        let entry = create_entry(Specific::Block(DeviceData { major: 1, minor: 2 }), 0o660);
        assert_eq!(file_type_and_permissions(&entry), "brw-rw----");

        // 5. Character device
        let entry = create_entry(
            Specific::Character(DeviceData { major: 1, minor: 2 }),
            0o600,
        );
        assert_eq!(file_type_and_permissions(&entry), "crw-------");

        // 6. FIFO
        let entry = create_entry(Specific::Fifo(PROTO_NULL_VALUE), 0o644);
        assert_eq!(file_type_and_permissions(&entry), "|rw-r--r--");

        // 7. Socket
        let entry = create_entry(Specific::Socket(PROTO_NULL_VALUE), 0o755);
        assert_eq!(file_type_and_permissions(&entry), "srwxr-xr-x");

        // 8. SUID
        // Executable
        let entry = create_entry(
            Specific::Regular(RegularData {
                size: 10,
                hash: vec![],
            }),
            0o4755,
        );
        assert_eq!(file_type_and_permissions(&entry), ".rwsr-xr-x");
        // Non-executable
        let entry = create_entry(
            Specific::Regular(RegularData {
                size: 10,
                hash: vec![],
            }),
            0o4655,
        );
        assert_eq!(file_type_and_permissions(&entry), ".rwSr-xr-x");

        // 9. SGID
        // Executable
        let entry = create_entry(
            Specific::Regular(RegularData {
                size: 10,
                hash: vec![],
            }),
            0o2755,
        );
        assert_eq!(file_type_and_permissions(&entry), ".rwxr-sr-x");
        // Non-executable
        let entry = create_entry(
            Specific::Regular(RegularData {
                size: 10,
                hash: vec![],
            }),
            0o2745,
        );
        assert_eq!(file_type_and_permissions(&entry), ".rwxr-Sr-x");

        // 10. Sticky bit
        // Executable (others have x)
        let entry = create_entry(
            Specific::Directory(DirectoryData { content: vec![] }),
            0o1755,
        );
        assert_eq!(file_type_and_permissions(&entry), "drwxr-xr-t");
        // Non-executable (others don't have x)
        let entry = create_entry(
            Specific::Directory(DirectoryData { content: vec![] }),
            0o1754,
        );
        assert_eq!(file_type_and_permissions(&entry), "drwxr-xr-T");
    }
}
