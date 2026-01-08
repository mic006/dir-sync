//! Filesystem related helpers

use std::fs::File;
use std::path::Path;

use bytes::{Buf, BufMut, BytesMut};

/// Extension of `std::path::Path`
pub trait PathExt {
    /// Convert path to &str, giving a `anyhow::Result`
    ///
    /// # Errors
    /// * path is not valid UTF8
    fn checked_as_str(&self) -> anyhow::Result<&str>;

    /// Get filename as &str, giving a `anyhow::Result`
    ///
    /// # Errors
    /// * filename is not valid UTF8
    /// * no filename
    fn file_name_as_str(&self) -> anyhow::Result<&str>;
}
impl PathExt for Path {
    fn checked_as_str(&self) -> anyhow::Result<&str> {
        self.to_str()
            .ok_or_else(|| anyhow::anyhow!("Path is not valid UTF8: {}", self.to_string_lossy()))
    }

    fn file_name_as_str(&self) -> anyhow::Result<&str> {
        self.file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or_else(|| {
                anyhow::anyhow!("No valid UTF8 filename in path: {}", self.to_string_lossy())
            })
    }
}

/// Extension of `prost::Message`
pub trait MessageExt: Sized {
    /// Load message from file
    ///
    /// # Errors
    /// * IO error, like invalid path or insufficient permissions
    /// * Conversion error
    fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self>;
    /// Save message to file
    ///
    /// # Errors
    /// * IO error, like invalid path or insufficient permissions
    fn save_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()>;
}
impl<M: prost::Message + Default> MessageExt for M {
    fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let f = File::open(path)?;
        let mut c = zstd::stream::Decoder::new(f)?;
        let mut b = BytesMut::new().writer();
        std::io::copy(&mut c, &mut b)?;
        Ok(Self::decode(b.into_inner())?)
    }

    fn save_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let f = File::create(path)?;
        let mut c = zstd::stream::Encoder::new(f, 0)?;
        let mut buf = BytesMut::new();
        self.encode(&mut buf)?;
        std::io::copy(&mut buf.reader(), &mut c)?;
        c.finish()?;
        Ok(())
    }
}

/// Path escaping, like `systemd-escape --path`.
/// Code from <https://github.com/lucab/libsystemd-rs/blob/master/src/unit.rs>
#[must_use]
pub fn systemd_escape_path(name: &str) -> String {
    #[must_use]
    fn systemd_escape_byte(b: u8, index: usize) -> String {
        let c = char::from(b);
        match c {
            '/' => '-'.to_string(),
            ':' | '_' | '0'..='9' | 'a'..='z' | 'A'..='Z' => c.to_string(),
            '.' if index > 0 => c.to_string(),
            _ => format!(r"\x{b:02x}"),
        }
    }

    let trimmed = name.trim_matches('/');
    if trimmed.is_empty() {
        return "-".to_string();
    }

    let mut slash_seq = false;
    let parts: Vec<String> = trimmed
        .bytes()
        .filter(|b| {
            let is_slash = *b == b'/';
            let res = !(is_slash && slash_seq);
            slash_seq = is_slash;
            res
        })
        .enumerate()
        .map(|(n, b)| systemd_escape_byte(b, n))
        .collect();
    parts.join("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_checked_as_str_success() {
        assert_eq!(
            Path::new("/some/path_with/non_ascii/€/char")
                .checked_as_str()
                .unwrap(),
            "/some/path_with/non_ascii/€/char"
        );
    }

    #[test]
    fn path_file_name_as_str_success() {
        assert_eq!(
            Path::new("/some/path_with/non_ascii/€uro")
                .file_name_as_str()
                .unwrap(),
            "€uro"
        );
    }

    #[derive(PartialEq, prost::Message, Clone)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        pub txt: String,
        #[prost(uint32, tag = "2")]
        pub value: u32,
    }

    #[test]
    fn message_load_save() -> anyhow::Result<()> {
        let test_dir = tempfile::tempdir()?;

        let src = TestMessage {
            txt: "Some text".into(),
            value: 42,
        };

        let p = test_dir.path().join("test_msg.pb.bin.zst");
        src.save_to_file(&p)?;

        let read = TestMessage::load_from_file(&p)?;

        assert_eq!(read, src);

        Ok(())
    }
}
