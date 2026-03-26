//! Diff operation on strings

/// Autonomous reference to a substring
/// Use offset and length, to avoid lifetime issues
/// The slice is still related to the original string, user shall not mix `StrSlice` with different strings
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct StrSlice {
    /// Offset in the original string
    offset: usize,
    /// Length of the substring
    length: usize,
}
impl StrSlice {
    /// Get the substring from the original string
    #[must_use]
    pub fn get<'a>(&self, original: &'a str) -> &'a str {
        &original[self.offset..self.offset + self.length]
    }

    /// Extend the length of the substring
    fn extend(&mut self, extra_length: usize) {
        self.length += extra_length;
    }
}

/// Difference chunk kind
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DiffChunkType {
    /// Common content with counterpart
    Common,
    /// Different content compared to counterpart
    Differ,
}

/// Difference chunk
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct DiffChunk {
    pub kind: DiffChunkType,
    pub slice: StrSlice,
}

/// Vector of difference chunks (one line with intra-line differences)
#[derive(Debug, PartialEq, Default)]
pub struct DiffChunkVec(pub Vec<DiffChunk>);
impl DiffChunkVec {
    fn append(&mut self, chunk: DiffChunk) {
        if let Some(last_chunk) = self.0.last_mut()
            && last_chunk.kind == chunk.kind
        {
            last_chunk.slice.extend(chunk.slice.length);
        } else {
            self.0.push(chunk);
        }
    }
}

/// Compared two ascii strings with same length, one char at a time
#[must_use]
pub fn diff_fixed_ascii_str(a: &str, b: &str) -> (DiffChunkVec, DiffChunkVec) {
    assert_eq!(
        a.len(),
        b.len(),
        "the two strings must have the same length"
    );

    let mut diff_a = DiffChunkVec::default();
    let mut diff_b = DiffChunkVec::default();

    for (i, (a, b)) in std::iter::zip(a.chars(), b.chars()).enumerate() {
        let chunk = DiffChunk {
            kind: if a == b {
                DiffChunkType::Common
            } else {
                DiffChunkType::Differ
            },
            slice: StrSlice {
                offset: i,
                length: 1,
            },
        };
        diff_a.append(chunk);
        diff_b.append(chunk);
    }
    (diff_a, diff_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    impl DiffChunk {
        fn debug<'a>(&self, original: &'a str) -> (DiffChunkType, &'a str) {
            (self.kind, self.slice.get(original))
        }
    }

    impl DiffChunkVec {
        fn debug<'a>(&self, original: &'a str) -> Vec<(DiffChunkType, &'a str)> {
            self.0.iter().map(|chunk| chunk.debug(original)).collect()
        }
    }

    #[test]
    fn test_diff_fixed_ascii_str() {
        let a = "drwxrwxr--";
        let b = "dr-xr--r--";
        let (da, db) = diff_fixed_ascii_str(a, b);

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
        assert_eq!(da.debug(a), expected_a);

        let expected_b = vec![
            (DiffChunkType::Common, "dr"),
            (DiffChunkType::Differ, "-"),
            (DiffChunkType::Common, "xr"),
            (DiffChunkType::Differ, "--"),
            (DiffChunkType::Common, "r--"),
        ];
        assert_eq!(db.debug(b), expected_b);
    }
}
