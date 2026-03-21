//! Rendering of TUI application on terminal

use std::rc::Rc;

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
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
        let [top_bar_area, list_area, middle_bar_area, content_area] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .areas(area);

        self.render_screen_normal_top_bar(top_bar_area, buf);
        self.render_screen_normal_list(list_area, buf);
        self.render_screen_normal_middle_bar(middle_bar_area, buf);
        self.render_screen_normal_content(content_area, buf);
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
    fn render_screen_normal_top_bar(&self, area: Rect, buf: &mut Buffer) {
        let style = &self.theme.main.bars;

        let title = if self.view.is_diff() {
            " dir-diff "
        } else {
            " dir-sync "
        };

        let [left_area, middle_area, right_area] = Layout::horizontal([
            Constraint::Fill(1),
            Constraint::Length(title.len() as u16),
            Constraint::Fill(1),
        ])
        .areas(area);

        // middle
        Line::from(Span::styled(title, &self.theme.main.title))
            .style(style)
            .render(middle_area, buf);

        // left side
        let left_str = match self.view {
            View::Browsing => "Browsing directories…",
            View::Diff => "Differences",
            View::SyncAll => "All differences",
            View::SyncConflicts => "Conflicts",
            View::SyncResolved => "Resolved",
            View::Syncing => "Synchronizing directories…",
        };
        let left_str = if self.view.are_diffs_rendered() {
            format!("{left_str} [a-b/c]") // TODO: fill a/b/c
        } else {
            left_str.into()
        };
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

    /// Render list of diffs of normal screen
    ///
    /// - display part of the list, fitting the area
    /// - highlight selected item
    /// - display scroll bar
    #[allow(clippy::unused_self)]
    fn render_screen_normal_list(&self, _area: Rect, _buf: &mut Buffer) {}

    /// Render middle bar of normal screen
    ///
    /// Name of each tree on top of its content area
    fn render_screen_normal_middle_bar(&self, area: Rect, buf: &mut Buffer) {
        let areas = self.split_area_per_tree(area, buf);
        std::iter::zip(areas.iter(), self.arg.dirs.iter()).for_each(|(area, dir)| {
            Line::styled(dir, &self.theme.main.bars)
                .centered()
                .render(*area, buf);
        });
    }

    /// Render content of each tree on normal screen
    ///
    /// - display metadata of the selected item for each tree
    /// - display content of the selected item for each tree
    /// - highlight differences between the trees
    /// - display scroll bar
    fn render_screen_normal_content(&self, area: Rect, buf: &mut Buffer) {
        let _areas = self.split_area_per_tree(area, buf);
    }

    /// Split one area into sub-area, one for each tree
    ///
    /// Fill spacers with bar style
    fn split_area_per_tree(&self, area: Rect, buf: &mut Buffer) -> Rc<[Rect]> {
        let (areas, spacers) =
            Layout::horizontal(std::iter::repeat_n(Constraint::Fill(1), self.nb_trees()))
                .spacing(1)
                .split_with_spacers(area);

        // fill spacers with bar style
        for spacer in spacers.iter() {
            buf.set_style(*spacer, &self.theme.main.bars);
        }
        areas
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

    /// Number of trees to be displayed
    fn nb_trees(&self) -> usize {
        self.arg.dirs.len()
    }
}
