//! Help screen

use unicode_width::UnicodeWidthStr;

use crate::tui::rich_text::RichText;

/// Help context
pub struct Help {
    /// Help content, per line
    pub content: Vec<RichText>,
    /// Display width of content = max(line.width())
    pub width: u16,
}

/// Help text with markdown formatting
const HELP_TEXT: &str = include_str!("../../tui_help.txt");

impl Help {
    pub fn new() -> Self {
        let content = RichText::from_markdown(HELP_TEXT);
        let width = content.iter().map(UnicodeWidthStr::width).max().unwrap() as u16;

        Self { content, width }
    }
}
