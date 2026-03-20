//! Rendering of TUI application on terminal

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Rect},
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Clear, Padding, Paragraph, Widget},
};

use super::rich_text::{Effect, RichSpan, RichText};
use super::{App, View, help};

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
    fn render_screen_normal(&mut self, area: Rect, buf: &mut Buffer) {
        self.render_screen_normal_top_bar(area, buf);
    }

    /// Render top bar of normal screen
    ///
    /// Left side:
    /// - `View::Diff`: "Differences [a-b/c]"
    /// - `View::SyncAll`: "All differences [a-b/c]"
    /// - `View::SyncConflicts`: "Conflicts [a-b/c]"
    /// - `View::SyncResolved`: "Resolved [a-b/c]"
    ///
    /// Middle:
    /// - `View::Diff`: "dir-diff"
    /// - `View::Sync*`: "dir-sync"
    ///
    /// Right:
    /// - `View::Diff`: nothing
    /// - `View::Sync*`: shortcut keys for view with highlight of active view
    fn render_screen_normal_top_bar(&self, mut area: Rect, buf: &mut Buffer) {
        area.height = 1;
        let style = &self.theme.main.bars;

        let title = if self.view.is_diff() {
            " dir-diff "
        } else {
            " dir-sync "
        };

        /* split bar in 3
         * - middle part fits title
         * - left part from start to middle part
         * - right part from middle part to end
         */
        let middle_area = area.centered_horizontally(Constraint::Length(title.len() as u16));
        let left_area = Rect {
            height: 1,
            width: middle_area.left(),
            x: 0,
            y: 0,
        };
        let right_area = Rect {
            height: 1,
            width: area.width - middle_area.right(),
            x: middle_area.right(),
            y: 0,
        };

        // middle
        Line::from(Span::styled(title, &self.theme.main.title))
            .style(style)
            .render(middle_area, buf);

        // left side
        let left_str = match self.view {
            View::Diff => "Differences",
            View::SyncAll => "All differences",
            View::SyncConflicts => "Conflicts",
            View::SyncResolved => "Resolved",
        };
        let left_str = format!("{left_str} [a-b/c]"); // TODO: fill a/b/c
        Line::styled(left_str, style)
            .left_aligned()
            .render(left_area, buf);

        // right side
        if !self.view.is_diff() {
            let spans = [
                (View::SyncAll, "F5", "All diff"),
                (View::SyncConflicts, "F6", "Conflicts"),
                (View::SyncResolved, "F7", "Resolved"),
            ]
            .into_iter()
            .flat_map(|(view, key, desc)| {
                let style = if view == self.view {
                    &self.theme.main.active_view
                } else {
                    &self.theme.main.bars
                };
                [
                    Span::from(" "),
                    Span::styled(" ", style),
                    Span::styled(key, Style::from(style).patch(&self.theme.main.key_stroke)),
                    Span::styled(" ", style),
                    Span::styled(desc, style),
                    Span::styled(" ", style),
                ]
                .into_iter()
            })
            .collect::<Vec<_>>();
            Line::from(spans)
                .style(style)
                .right_aligned()
                .render(right_area, buf);
        }
    }

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
        let help_area = area.centered(
            Constraint::Length(help_width),
            Constraint::Length(help_height),
        );

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
        let dialog_area = area.centered(
            Constraint::Length(dialog_width),
            Constraint::Length(dialog_height),
        );

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
