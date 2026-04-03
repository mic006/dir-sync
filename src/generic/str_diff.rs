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
    #[must_use]
    pub fn new(kind: DiffChunkType, s: String) -> Self {
        Self(vec![DiffChunk { kind, s }])
    }

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
impl From<&str> for DiffLine {
    /// Build from string: one single chunk that differs
    fn from(s: &str) -> Self {
        Self(vec![DiffChunk {
            kind: DiffChunkType::Differ,
            s: s.into(),
        }])
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

/// Difference line with intra-line differences + line number
#[derive(Debug, PartialEq, Default)]
pub enum DiffLineNum {
    /// Padding line to align common parts for display
    #[default]
    Padding,
    Line {
        /// Line number of original content, 1-based
        line_num: usize,
        line: DiffLine,
    },
}

/// Multiline content with differences
#[derive(Debug, PartialEq)]
pub struct DiffMultiline {
    pub lines: Vec<DiffLineNum>,
}
impl From<&str> for DiffMultiline {
    /// Build from string: split by lines
    fn from(s: &str) -> Self {
        let lines = s
            .lines()
            .enumerate()
            .map(|(line_num, line)| DiffLineNum::Line {
                line_num: line_num + 1,
                line: line.into(),
            });
        DiffMultiline {
            lines: lines.collect(),
        }
    }
}

/// Builder for `DiffMultiline`, used by `diff_str`
#[derive(Debug)]
struct DiffMultilineBuilder {
    lines: Vec<DiffLineNum>,
    current_line: Option<DiffLine>,
    current_line_num: usize,
}
impl Default for DiffMultilineBuilder {
    fn default() -> Self {
        Self {
            lines: Vec::new(),
            current_line: None,
            current_line_num: 1,
        }
    }
}

impl DiffMultilineBuilder {
    /// Add string to the current line
    ///
    /// `s` doe nos contain line return
    fn append_str(&mut self, kind: DiffChunkType, s: &str) {
        if s.is_empty() {
            return;
        }
        self.current_line
            .get_or_insert_default()
            .append_str(kind, s);
    }

    /// Add end of line
    ///
    /// Add padding lines as needed to realign the display
    fn end_of_line(&mut self, display_line_num: Option<usize>) {
        let line = self.current_line.take().unwrap_or_default();
        self.lines.push(DiffLineNum::Line {
            line_num: self.current_line_num,
            line,
        });
        self.current_line_num += 1;
        if let Some(display_line_num) = display_line_num {
            self.lines.resize_with(display_line_num, Default::default);
        }
    }

    fn finalize(mut self, display_line_num: usize) -> DiffMultiline {
        self.end_of_line(Some(display_line_num));
        DiffMultiline { lines: self.lines }
    }

    fn nb_lines(&self) -> usize {
        self.lines.len()
    }
}

/// Wrapper around diff algorithm, with settings
///
/// If criteria are not met, the input strings are simply output as independent strings
/// with everything being different
struct DiffEngine {
    /// Minimum similarity between the 2 inputs
    /// Computed as length of common chunks / min length of inputs
    similar_ratio_min: f64,
}
impl DiffEngine {
    /// Compared two strings
    ///
    /// Return the content of each string, ready for side-by-side rendering
    /// If inputs are too different, the two strings are simply output as independent strings
    /// with everything being different
    pub fn diff(&self, a: &str, b: &str) -> (DiffMultiline, DiffMultiline) {
        if a.is_empty() || b.is_empty() {
            return (a.into(), b.into());
        }

        let chunks = dissimilar::diff(a, b);

        let mut diff_a = DiffMultilineBuilder::default();
        let mut diff_b = DiffMultilineBuilder::default();
        let mut common_count = 0;

        for chunk in chunks {
            match chunk {
                dissimilar::Chunk::Equal(s) => {
                    common_count += s.len();

                    for line in s.split_inclusive('\n') {
                        let line_break = line.ends_with('\n');
                        let line = if line_break {
                            &line[..line.len() - 1]
                        } else {
                            line
                        };
                        diff_a.append_str(DiffChunkType::Common, line);
                        diff_b.append_str(DiffChunkType::Common, line);
                        if line_break {
                            let display_line_num = diff_a.nb_lines().max(diff_b.nb_lines()) + 1;
                            diff_a.end_of_line(Some(display_line_num));
                            diff_b.end_of_line(Some(display_line_num));
                        }
                    }
                }
                dissimilar::Chunk::Delete(s) | dissimilar::Chunk::Insert(s) => {
                    let diff = if matches!(chunk, dissimilar::Chunk::Delete(_)) {
                        &mut diff_a
                    } else {
                        &mut diff_b
                    };
                    for line in s.split_inclusive('\n') {
                        let line_break = line.ends_with('\n');
                        let line = if line_break {
                            &line[..line.len() - 1]
                        } else {
                            line
                        };
                        diff.append_str(DiffChunkType::Differ, line);
                        if line_break {
                            diff.end_of_line(None);
                        }
                    }
                }
            }
        }

        #[allow(clippy::cast_precision_loss)]
        let similar_ratio = common_count as f64 / a.len().min(b.len()) as f64;
        if similar_ratio < self.similar_ratio_min {
            // inputs are too different
            return (a.into(), b.into());
        }

        let display_line_num = diff_a.nb_lines().max(diff_b.nb_lines()) + 1;
        (
            diff_a.finalize(display_line_num),
            diff_b.finalize(display_line_num),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type DiffChunkAssert<'a> = (DiffChunkType, &'a str);
    impl<'a> PartialEq<DiffChunkAssert<'a>> for DiffChunk {
        fn eq(&self, other: &DiffChunkAssert<'a>) -> bool {
            (self.kind, self.s.as_str()) == *other
        }
    }

    type DiffLineAssert<'a> = Vec<DiffChunkAssert<'a>>;
    impl<'a> PartialEq<DiffLineAssert<'a>> for DiffLine {
        fn eq(&self, other: &DiffLineAssert<'a>) -> bool {
            self.0 == *other
        }
    }

    type DiffMultilineAssert<'a> = Vec<(usize, DiffLineAssert<'a>)>;
    impl<'a> PartialEq<DiffMultilineAssert<'a>> for DiffMultiline {
        fn eq(&self, other: &DiffMultilineAssert<'a>) -> bool {
            self.lines.len() == other.len()
                && self
                    .lines
                    .iter()
                    .zip(other.iter())
                    .all(|(l, (num, o))| match l {
                        DiffLineNum::Padding => *num == 0,
                        DiffLineNum::Line { line_num, line } => line_num == num && line == o,
                    })
        }
    }

    #[test]
    fn test_diff_chunk_type_from_bool() {
        assert_eq!(DiffChunkType::from(true), DiffChunkType::Differ);
        assert_eq!(DiffChunkType::from(false), DiffChunkType::Common);
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
        assert_eq!(diff_a, expected_a);

        let expected_b = vec![
            (DiffChunkType::Common, "dr"),
            (DiffChunkType::Differ, "-"),
            (DiffChunkType::Common, "xr"),
            (DiffChunkType::Differ, "--"),
            (DiffChunkType::Common, "r--"),
        ];
        assert_eq!(diff_b, expected_b);
    }

    #[test]
    fn test_diff_line_append_and_merge() {
        let mut line = DiffLine::default();
        line.append_str(DiffChunkType::Common, "abc");
        line.append_str(DiffChunkType::Common, "def"); // Should merge
        line.append_chr(DiffChunkType::Differ, 'g');
        line.append_str(DiffChunkType::Differ, "hi"); // Should merge

        let expected = vec![
            (DiffChunkType::Common, "abcdef"),
            (DiffChunkType::Differ, "ghi"),
        ];
        assert_eq!(line, expected);
    }

    #[test]
    fn test_diff_engine_similar() {
        let engine = DiffEngine {
            similar_ratio_min: 0.5,
        };
        let a = "Hello World\nThis is a test";
        let b = "Hello Rust\nThis is a test";

        let (diff_a, diff_b) = engine.diff(a, b);

        let expected_a = vec![
            (
                1,
                vec![
                    (DiffChunkType::Common, "Hello "),
                    (DiffChunkType::Differ, "World"),
                ],
            ),
            (2, vec![(DiffChunkType::Common, "This is a test")]),
        ];
        let expected_b = vec![
            (
                1,
                vec![
                    (DiffChunkType::Common, "Hello "),
                    (DiffChunkType::Differ, "Rust"),
                ],
            ),
            (2, vec![(DiffChunkType::Common, "This is a test")]),
        ];
        assert_eq!(diff_a, expected_a);
        assert_eq!(diff_b, expected_b);
    }

    #[test]
    fn test_diff_engine_too_different() {
        let engine = DiffEngine {
            similar_ratio_min: 0.9, // Very strict
        };
        let a = "Short";
        let b = "Completely different and long string";

        let (diff_a, diff_b) = engine.diff(a, b);

        let expected_a = vec![(1, vec![(DiffChunkType::Differ, "Short")])];
        let expected_b = vec![(
            1,
            vec![(
                DiffChunkType::Differ,
                "Completely different and long string",
            )],
        )];
        assert_eq!(diff_a, expected_a);
        assert_eq!(diff_b, expected_b);
    }
}
