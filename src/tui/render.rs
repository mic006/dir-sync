//! Rendering of TUI application on terminal

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Clear, Padding, Paragraph, Widget},
};

use super::App;
use super::help;
use super::rich_text::{Effect, RichSpan, RichText};

impl Widget for &mut App {
    fn render(self, area: Rect, buf: &mut Buffer)
    where
        Self: Sized,
    {
        match self.screen {
            super::Screen::Normal => self.render_screen_normal(area, buf),
            super::Screen::Help => self.render_screen_help(area, buf),
            super::Screen::ConfirmExit => self.render_screen_confirm_exit(area, buf),
        }
    }
}

impl App {
    /// Render normal screen
    #[allow(clippy::unused_self)]
    fn render_screen_normal(&mut self, _area: Rect, _buf: &mut Buffer) {}

    /// Render Help screen
    ///
    /// - render normal screen
    /// - display help message in a block on top
    fn render_screen_help(&mut self, area: Rect, buf: &mut Buffer) {
        if self.help.is_none() {
            // get help content
            self.help = Some(help::Help::new());
        }

        // render normal screen first
        self.render_screen_normal(area, buf);

        // determine help area (centered)
        let help = self.help.as_ref().unwrap();
        // take borders into account
        let help_width = (help.width + 2).min(area.width);
        let help_height = ((help.content.len() + 2) as u16).min(area.height);
        let help_area = Rect {
            x: (area.width - help_width) / 2,
            y: (area.height - help_height) / 2,
            width: help_width,
            height: help_height,
        };

        // render help area
        Clear.render(help_area, buf);
        let help_block = Block::bordered()
            .border_style(&self.theme.help.border_style)
            .border_type(*self.theme.help.border_type)
            .style(&self.theme.help.content)
            .title(" Help ")
            .title_bottom(Line::from(" Any key to exit help screen ").right_aligned());
        let help_paragraph = Paragraph::new(self.format_help(&help.content)).block(help_block);
        help_paragraph.render(help_area, buf);
    }

    /// Render Exit confirmation screen
    ///
    /// - render normal screen
    /// - display confirm exit dialog box on top
    ///
    /// 3 buttons:
    /// - quit without syncing
    /// - sync
    /// - cancel
    fn render_screen_confirm_exit(&mut self, area: Rect, buf: &mut Buffer) {
        // render normal screen first
        self.render_screen_normal(area, buf);

        let msg = "Choose exit action";
        let button_quit = "Quit without syncing";
        let button_sync = "Synchronize and exit";

        // determine dialog box area (centered)
        // take borders into account
        let dialog_width = (50 + 2).min(area.width);
        let dialog_height = (3 + 2).min(area.height);
        let dialog_area = Rect {
            x: (area.width - dialog_width) / 2,
            y: (area.height - dialog_height) / 2,
            width: dialog_width,
            height: dialog_height,
        };

        // render dialog area
        Clear.render(dialog_area, buf);
        let dialog_block = Block::bordered()
            .border_style(&self.theme.confirm_exit.border_style)
            .border_type(*self.theme.confirm_exit.border_type)
            .style(&self.theme.confirm_exit.content)
            .padding(Padding::uniform(1))
            .title(" Exit ? ")
            .title_bottom(Self::format_button(button_sync).left_aligned())
            .title_bottom(Self::format_button(button_quit).right_aligned());
        let dialog_paragraph = Paragraph::new(msg).block(dialog_block);
        dialog_paragraph.render(dialog_area, buf);
    }

    /// Format one button
    ///
    /// Text appears in reversed mode, with the first letter (ASCII) in bold + underlined
    fn format_button(text: &str) -> Line<'_> {
        let style = Style::default().add_modifier(Modifier::REVERSED);
        vec![
            Span::from(" "),
            Span::styled(" ", style),
            Span::styled(
                &text[0..1],
                style.add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
            ),
            Span::styled(&text[1..], style),
            Span::styled(" ", style),
            Span::from(" "),
        ]
        .into()
    }

    /// Format help content
    fn format_help<'a>(&self, txt: &'a [RichText]) -> Text<'a> {
        let lines = txt
            .iter()
            .map(|t| self.format_help_line(t))
            .collect::<Vec<_>>();
        Text::from(lines)
    }

    /// Format one line of help
    fn format_help_line<'a>(&self, txt: &'a RichText) -> Line<'a> {
        let spans = txt
            .spans
            .iter()
            .map(|t| self.format_help_span(t))
            .collect::<Vec<_>>();
        Line::from(spans)
    }

    /// Format one span of help
    #[allow(clippy::unused_self)]
    fn format_help_span<'a>(&self, txt: &'a RichSpan) -> Span<'a> {
        let style = match txt.effect {
            Effect::None => Style::default(),
            Effect::Bold => Style::default().add_modifier(Modifier::BOLD),
            Effect::Italic => Style::default().add_modifier(Modifier::ITALIC),
            Effect::Code => Style::from(&self.theme.help.content_key_stroke),
            Effect::Highlight => Style::from(&self.theme.help.content_highlight),
        };
        Span::styled(txt.text.as_str(), style)
    }
}
