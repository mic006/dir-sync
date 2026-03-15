//! Manipulate text with formatting

use std::sync::LazyLock;

use regex::Regex;

use crate::generic::str_or_string::StrOrString;

/// Effects supported by `RichText`
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum Effect {
    None,
    /// Text in bold (**text** in markdown)
    Bold,
    /// Text in italic (__text__ in markdown)
    Italic,
    /// Code text (`text` in markdown)
    Code,
    /// Highlighted text (==text== in markdown)
    Highlight,
}
impl Effect {
    /// Get pattern associated to effect
    const fn pattern(&self) -> &'static str {
        match self {
            Effect::None => "",
            Effect::Bold => "**",
            Effect::Italic => "__",
            Effect::Code => "`",
            Effect::Highlight => "==",
        }
    }

    /// Get effect from pattern
    fn from_pattern(pattern: &str) -> Self {
        match pattern {
            "**" => Effect::Bold,
            "__" => Effect::Italic,
            "`" => Effect::Code,
            "==" => Effect::Highlight,
            _ => Effect::None,
        }
    }
}

/// Text with formatting
#[derive(PartialEq, Debug, Default)]
pub struct RichText {
    /// Each element one span of text with its formatting
    spans: Vec<(Effect, StrOrString)>,
}

impl RichText {
    /// Process a markdown formatted string
    ///
    /// Returns a vector of `RichText`, one entry per line
    #[must_use]
    pub fn from_markdown(s: &str) -> Vec<Self> {
        s.lines().map(Self::from_markdown_line).collect()
    }

    fn from_markdown_line(s: &str) -> Self {
        // identify the different patterns
        static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?:\*\*|__|`|==)").unwrap());

        let mut effect = Effect::None;
        let mut spans = vec![];

        let mut previous = 0;
        for m in RE.find_iter(s) {
            if m.start() > previous {
                let text = &s[previous..m.start()];
                spans.push((effect, StrOrString::from_str_to_owned(text)));
            }
            let new_effect = Effect::from_pattern(m.as_str());
            if effect == Effect::None {
                // starting new effect
                effect = new_effect;
            } else {
                if effect != new_effect {
                    return Self::from_error(s, "nested or unmatched pattern");
                }
                // ending of effect
                effect = Effect::None;
            }
            previous = m.end();
        }
        if effect != Effect::None {
            return Self::from_error(s, "unclosed pattern");
        }
        if previous < s.len() {
            let text = &s[previous..];
            spans.push((Effect::None, StrOrString::from_str_to_owned(text)));
        }

        Self { spans }
    }

    fn from_error(s: &str, err: &str) -> Self {
        log::error!("{err} in markdown: {s}; returning raw text");
        Self {
            spans: vec![(Effect::None, StrOrString::from_str_to_owned(s))],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generic::str_or_string::StrOrString;

    #[test]
    fn test_plain_text() {
        let text = RichText::from_markdown_line("Hello world");
        assert_eq!(
            text,
            RichText {
                spans: vec![(Effect::None, StrOrString::String("Hello world".to_owned()))]
            }
        );
    }

    #[test]
    fn test_simple_effects() {
        let cases = vec![
            (
                "**bold**",
                vec![(Effect::Bold, StrOrString::String("bold".to_owned()))],
            ),
            (
                "__italic__",
                vec![(Effect::Italic, StrOrString::String("italic".to_owned()))],
            ),
            (
                "`code`",
                vec![(Effect::Code, StrOrString::String("code".to_owned()))],
            ),
            (
                "==highlight==",
                vec![(
                    Effect::Highlight,
                    StrOrString::String("highlight".to_owned()),
                )],
            ),
        ];

        for (input, expected_spans) in cases {
            let text = RichText::from_markdown_line(input);
            assert_eq!(text.spans, expected_spans, "Failed for input: {input}");
        }
    }

    #[test]
    fn test_mixed_effects() {
        let text = RichText::from_markdown_line("Hello **bold** and __italic__ world");
        assert_eq!(
            text.spans,
            vec![
                (Effect::None, StrOrString::String("Hello ".to_owned())),
                (Effect::Bold, StrOrString::String("bold".to_owned())),
                (Effect::None, StrOrString::String(" and ".to_owned())),
                (Effect::Italic, StrOrString::String("italic".to_owned())),
                (Effect::None, StrOrString::String(" world".to_owned())),
            ]
        );
    }

    #[test]
    fn test_empty_effect() {
        // empty bold **** should result in empty spans (or skipped)
        let rt = RichText::from_markdown_line("****");
        assert_eq!(rt, RichText { spans: vec![] });

        let rt = RichText::from_markdown_line("a****b");
        assert_eq!(
            rt,
            RichText {
                spans: vec![
                    (Effect::None, StrOrString::String("a".to_owned())),
                    (Effect::None, StrOrString::String("b".to_owned()))
                ]
            }
        );
    }

    #[test]
    fn test_multiline() {
        let text = RichText::from_markdown("Line 1\n**Line 2**");
        assert_eq!(text.len(), 2);
        assert_eq!(
            text[0].spans,
            vec![(Effect::None, StrOrString::String("Line 1".to_owned()))]
        );
        assert_eq!(
            text[1].spans,
            vec![(Effect::Bold, StrOrString::String("Line 2".to_owned()))]
        );
    }

    #[test]
    fn test_errors() {
        crate::generic::test::log_init();

        // Unclosed
        let text = RichText::from_markdown_line("**bold");
        assert_eq!(
            text.spans,
            vec![(Effect::None, StrOrString::String("**bold".to_owned()))]
        );

        // Nested / unmatched
        let text = RichText::from_markdown_line("**bo__ld**");
        assert_eq!(
            text.spans,
            vec![(Effect::None, StrOrString::String("**bo__ld**".to_owned()))]
        );
    }
}
