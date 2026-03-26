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
impl DiffChunk {
    /// Get chunk type and substring from the original string
    #[must_use]
    fn get<'a>(&self, original: &'a str) -> (DiffChunkType, &'a str) {
        (self.kind, self.slice.get(original))
    }
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

    /// Get iterator over chunks
    pub fn chunks<'a>(&self, original: &'a str) -> impl Iterator<Item = (DiffChunkType, &'a str)> {
        self.0.iter().map(|chunk| chunk.get(original))
    }
}

/// Compared two ascii strings with same length, one char at a time
///
/// The output mask is applicable to both a input strings
#[must_use]
pub fn diff_fixed_ascii_str(a: &str, b: &str) -> DiffChunkVec {
    assert_eq!(
        a.len(),
        b.len(),
        "the two strings must have the same length"
    );

    let mut diff = DiffChunkVec::default();

    for (i, (a, b)) in std::iter::zip(a.chars(), b.chars()).enumerate() {
        let kind = if a == b {
            DiffChunkType::Common
        } else {
            DiffChunkType::Differ
        };
        diff.append(DiffChunk {
            kind,
            slice: StrSlice {
                offset: i,
                length: 1,
            },
        });
    }
    diff
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diff_fixed_ascii_str() {
        let a = "drwxrwxr--";
        let b = "dr-xr--r--";
        let d = diff_fixed_ascii_str(a, b);

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
        assert_eq!(d.chunks(a).collect::<Vec<_>>(), expected_a);

        let expected_b = vec![
            (DiffChunkType::Common, "dr"),
            (DiffChunkType::Differ, "-"),
            (DiffChunkType::Common, "xr"),
            (DiffChunkType::Differ, "--"),
            (DiffChunkType::Common, "r--"),
        ];
        assert_eq!(d.chunks(b).collect::<Vec<_>>(), expected_b);
    }
}
