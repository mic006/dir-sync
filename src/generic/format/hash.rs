//! Pretty print file hash

use crate::proto::{MyDirEntry, Specific};

/// Print file hash in compact format
#[must_use]
pub fn format_hash(entry: &MyDirEntry) -> String {
    let res = if let Some(Specific::Regular(file_data)) = &entry.specific {
        if file_data.size == 0 {
            None
        } else if file_data.hash.len() == 32 {
            Some(format!(
                "{:02x}{:02x}…{:02x}{:02x}",
                &file_data.hash[0], &file_data.hash[1], &file_data.hash[30], &file_data.hash[31],
            ))
        } else {
            Some(String::from("!Invalid!"))
        }
    } else {
        // not relevant for other types of files
        None
    };
    res.unwrap_or_else(|| String::from("        -"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{DirectoryData, RegularData};

    #[test]
    fn test_format_hash() {
        // Helper to create a dummy entry
        let create_entry = |specific: Specific| -> MyDirEntry {
            MyDirEntry {
                file_name: "test".to_string(),
                permissions: 0,
                uid: 0,
                gid: 0,
                mtime: None,
                specific: Some(specific),
            }
        };

        // 1. Regular file, valid hash
        let hash: Vec<u8> = (0..32).collect();
        let entry = create_entry(Specific::Regular(RegularData { size: 100, hash }));
        // 0x00, 0x01 ... 0x1E, 0x1F
        assert_eq!(format_hash(&entry), "0001…1e1f");

        // 2. Regular file, empty (size 0)
        let entry = create_entry(Specific::Regular(RegularData {
            size: 0,
            hash: vec![],
        }));
        assert_eq!(format_hash(&entry), "        -");

        // 3. Regular file, invalid hash length
        let entry = create_entry(Specific::Regular(RegularData {
            size: 100,
            hash: vec![0; 10],
        }));
        assert_eq!(format_hash(&entry), "!Invalid!");

        // 4. Directory (not regular)
        let entry = create_entry(Specific::Directory(DirectoryData { content: vec![] }));
        assert_eq!(format_hash(&entry), "        -");
    }
}
