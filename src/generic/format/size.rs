//! Pretty print file size

use crate::proto::{MyDirEntry, Specific};

/// Print file size in human readable format
#[must_use]
pub fn format_file_size(entry: &MyDirEntry) -> String {
    if let Some(Specific::Regular(file_data)) = &entry.specific {
        format_size(file_data.size)
    } else {
        // not relevant for other types of files
        String::from("   -")
    }
}

/// Get file size in human readable format
///
/// Use exactly 4 characters
/// - 3 characters for the number; may be fractional if < 10, like 3.5
/// - size suffix
#[must_use]
fn format_size(sz: u64) -> String {
    const SUFFIXES: [char; 7] = ['b', 'k', 'M', 'G', 'T', 'P', 'E'];
    #[allow(clippy::cast_precision_loss)]
    let mut ft_sz = sz as f64;
    let mut suffix = 0_usize;

    while ft_sz >= 999.5 {
        ft_sz /= 1024.0;
        suffix += 1;
    }
    let suffix_c = SUFFIXES[suffix];

    let res = if suffix == 0 {
        // exact file size
        format!("{ft_sz:>3.0}b")
    } else if ft_sz < 9.95 {
        // single digit, fractional part
        format!("{ft_sz:>3.1}{suffix_c}")
    } else {
        // 2 or 3 digits, no fractional part
        format!("{ft_sz:>3.0}{suffix_c}")
    };
    debug_assert!(res.len() == 4);
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{MyDirEntry, Specific};

    #[test]
    fn test_format_file_size() {
        let mut entry = MyDirEntry {
            file_name: "test".to_string(),
            permissions: 0,
            uid: 0,
            gid: 0,
            mtime: None,
            specific: Some(Specific::Regular(crate::proto::RegularData {
                size: 12345,
                hash: vec![],
            })),
        };
        assert_eq!(format_file_size(&entry), " 12k");

        // Non-regular file
        entry.specific = Some(Specific::Directory(crate::proto::DirectoryData {
            content: vec![],
        }));
        assert_eq!(format_file_size(&entry), "   -");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "  0b");
        assert_eq!(format_size(9), "  9b");
        assert_eq!(format_size(10), " 10b");
        assert_eq!(format_size(999), "999b");
        assert_eq!(format_size(1000), "1.0k");
        assert_eq!(format_size(1023), "1.0k");
        assert_eq!(format_size(1024), "1.0k");
        assert_eq!(format_size(10188), "9.9k"); // 9.949...
        assert_eq!(format_size(10189), " 10k"); // 9.950...
        assert_eq!(format_size(10 * 1024), " 10k");
        assert_eq!(format_size(999 * 1024 + 511), "999k"); // 999.49...
        assert_eq!(format_size(999 * 1024 + 512), "1.0M"); // 999.50...
        assert_eq!(format_size(1024 * 1024 - 1), "1.0M");
        assert_eq!(format_size(1024 * 1024), "1.0M");
        assert_eq!(format_size(5 * 1024 * 1024), "5.0M");
        assert_eq!(format_size(1024 * 1024 * 1024), "1.0G");
        assert_eq!(format_size(1024_u64.pow(4)), "1.0T");
        assert_eq!(format_size(1024_u64.pow(5)), "1.0P");
        assert_eq!(format_size(1024_u64.pow(6)), "1.0E");
        assert_eq!(format_size(u64::MAX), " 16E");
    }
}
