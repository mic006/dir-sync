//! Diff operation on strings

/// Difference chunk kind
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DiffChunkType {
    /// Common content with counterpart
    Common,
    /// Different content compared to counterpart
    Differ,
}
impl From<bool> for DiffChunkType {
    fn from(b: bool) -> Self {
        if b {
            DiffChunkType::Differ
        } else {
            DiffChunkType::Common
        }
    }
}

/// Difference chunk
#[derive(Debug, PartialEq)]
pub struct DiffChunk {
    pub kind: DiffChunkType,
    pub s: String,
}

/// Difference line with intra-line differences (= vector of difference chunks)
#[derive(Debug, PartialEq, Default)]
pub struct DiffLine(pub Vec<DiffChunk>);
impl DiffLine {
    pub fn append_chr(&mut self, kind: DiffChunkType, c: char) {
        if let Some(last_chunk) = self.0.last_mut()
            && last_chunk.kind == kind
        {
            last_chunk.s.push(c);
        } else {
            self.0.push(DiffChunk { kind, s: c.into() });
        }
    }

    pub fn append_str(&mut self, kind: DiffChunkType, s: &str) {
        if let Some(last_chunk) = self.0.last_mut()
            && last_chunk.kind == kind
        {
            last_chunk.s.push_str(s);
        } else {
            self.0.push(DiffChunk { kind, s: s.into() });
        }
    }
}

/// Compared two ascii strings with same length, one char at a time
///
/// Output the `DiffLine` for a and b
#[must_use]
pub fn diff_fixed_ascii_str(a: &str, b: &str) -> (DiffLine, DiffLine) {
    assert_eq!(
        a.len(),
        b.len(),
        "the two strings must have the same length"
    );

    let mut diff_a = DiffLine::default();
    let mut diff_b = DiffLine::default();

    for (a, b) in std::iter::zip(a.chars(), b.chars()) {
        let kind = if a == b {
            DiffChunkType::Common
        } else {
            DiffChunkType::Differ
        };
        diff_a.append_chr(kind, a);
        diff_b.append_chr(kind, b);
    }
    (diff_a, diff_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    impl PartialEq<(DiffChunkType, &str)> for DiffChunk {
        fn eq(&self, other: &(DiffChunkType, &str)) -> bool {
            (self.kind, self.s.as_str()) == *other
        }
    }

    #[test]
    fn test_diff_fixed_ascii_str() {
        let a = "drwxrwxr--";
        let b = "dr-xr--r--";
        let (diff_a, diff_b) = diff_fixed_ascii_str(a, b);

        // Comparison map:
        // d r w x r w x r - - (a)
        // d r - x r - - r - - (b)
        // C C D C C D D C C C (Kind: Common/Differ)

        let expected_a = vec![
            (DiffChunkType::Common, "dr"),
            (DiffChunkType::Differ, "w"),
            (DiffChunkType::Common, "xr"),
            (DiffChunkType::Differ, "wx"),
            (DiffChunkType::Common, "r--"),
        ];
        assert_eq!(diff_a.0, expected_a);

        let expected_b = vec![
            (DiffChunkType::Common, "dr"),
            (DiffChunkType::Differ, "-"),
            (DiffChunkType::Common, "xr"),
            (DiffChunkType::Differ, "--"),
            (DiffChunkType::Common, "r--"),
        ];
        assert_eq!(diff_b.0, expected_b);
    }
}
