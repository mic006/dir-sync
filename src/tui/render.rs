//! Rendering of TUI application on terminal

use std::rc::Rc;

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Clear, Padding, Paragraph, Widget},
};

use crate::generic::str_diff::{DiffChunk, DiffChunkType, DiffLine};

use super::diff_context::{DiffEntryContextRender, NB_METADATA_LINES, RenderDiffType};
use super::rich_text::{Effect, RichSpan, RichText};
use super::theme::AppTheme;
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
        let (fill_factor_list, fill_factor_content) = self.theme.fill_factors();

        let [top_bar_area, list_area, middle_bar_area, content_area] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Fill(fill_factor_list),
            Constraint::Length(1),
            Constraint::Fill(fill_factor_content),
        ])
        .areas(area);

        self.render_screen_normal_list(list_area, buf);
        self.render_screen_normal_middle_bar(middle_bar_area, buf);
        self.render_screen_normal_content(content_area, buf);
        // render top bar at the end to have normalized list panel info
        self.render_screen_normal_top_bar(top_bar_area, buf);
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
        let bar_style = self.theme.bar_style();

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
        Line::styled(title, self.theme.bar_title_style()).render(middle_area, buf);

        // left side
        let left_str = match self.view {
            View::Browsing => "Browsing directories…",
            View::Diff => "Differences",
            View::SyncAll => "All differences",
            View::SyncConflicts => "Conflicts",
            View::SyncResolved => "Resolved",
            View::Syncing => "Synchronizing directories…",
        };
        let left_str = if let Some(context) = &self.context {
            let view_range = context.list_panel.view_range();
            if context.list_panel.content_length == 0 {
                format!("{left_str} [0/0]")
            } else {
                format!(
                    "{left_str} [{}-{}/{}]",
                    view_range.start + 1, // human count, 1-based display
                    view_range.end,
                    context.list_panel.content_length
                )
            }
        } else {
            left_str.into()
        };
        Line::styled(left_str, bar_style)
            .left_aligned()
            .render(left_area, buf);

        // right side
        if self.view.is_diff() {
            buf.set_style(right_area, bar_style);
        } else {
            let spans = [
                (View::SyncAll, "F5", "All diff"),
                (View::SyncConflicts, "F6", "Conflicts"),
                (View::SyncResolved, "F7", "Resolved"),
            ]
            .into_iter()
            .flat_map(|(view, key, desc)| {
                let view_style = if view == self.view {
                    self.theme.bar_active_view_style()
                } else {
                    bar_style
                };
                [
                    Span::from(" "),
                    Span::styled(" ", view_style),
                    Span::styled(
                        key,
                        view_style.patch(self.theme.bar_key_stroke_style_patch()),
                    ),
                    Span::styled(" ", view_style),
                    Span::styled(desc, view_style),
                    Span::styled(" ", view_style),
                ]
                .into_iter()
            })
            .collect::<Vec<_>>();
            Line::from(spans)
                .style(bar_style)
                .right_aligned()
                .render(right_area, buf);
        }
    }

    /// Render list of diffs of normal screen
    ///
    /// - display part of the list, fitting the area
    /// - highlight selected item
    /// - display scroll bar
    fn render_screen_normal_list(&mut self, area: Rect, buf: &mut Buffer) {
        if let Some(context) = &mut self.context {
            context.list_panel.normalize(area.height.into());

            // render content
            for (i, row) in std::iter::zip(context.list_panel.view_range(), area.rows()) {
                let style = if i == context.list_panel.selected {
                    self.theme.diff_list_selected_item_style()
                } else {
                    self.theme.content_style()
                };
                let index = context.get_diff_entry_index(self.view, i);
                let diff_entry = &context.diffs[index];
                let spans = match self.view {
                    View::Diff => {
                        vec![Span::from(format!("{diff_entry}"))]
                    }
                    View::SyncAll | View::SyncConflicts | View::SyncResolved => {
                        vec![
                            Span::from("["),
                            if let Some(sync_source_index) = &diff_entry.sync_source_index {
                                Span::styled(
                                    (sync_source_index + 1).to_string(),
                                    self.theme.diff_list_sync_resolved_style_patch(),
                                )
                            } else {
                                Span::styled("!", self.theme.diff_list_sync_conflict_style_patch())
                            },
                            Span::from(format!("] {diff_entry:}")),
                        ]
                    }
                    _ => unreachable!("context is invalid for the unhandled views"),
                };
                Line::from(spans).style(style).render(row, buf);
            }

            // render scroll bar on top of content
            if let Some(scroll) = context.list_panel.scroll_bar() {
                // render scroll bar
                let sb_style = self.theme.bar_scroll_bar_style();
                for (i, c) in scroll {
                    buf[(area.right() - 1, area.y + i as u16)]
                        .set_style(sb_style)
                        .set_char(c);
                }
            }
        }
    }

    /// Render middle bar of normal screen
    ///
    /// Name of each tree on top of its content area
    fn render_screen_normal_middle_bar(&self, area: Rect, buf: &mut Buffer) {
        let bar_style = self.theme.bar_style();
        let areas = self.split_area_per_tree(area, buf);
        for (i, (area, dir)) in std::iter::zip(areas.iter(), self.arg.dirs.iter()).enumerate() {
            Line::styled(format!("#{} {dir}", i + 1), bar_style)
                .centered()
                .render(*area, buf);
        }
    }

    /// Render content of each tree on normal screen
    ///
    /// - display metadata of the selected item for each tree
    /// - display content of the selected item for each tree
    /// - highlight differences between the trees
    /// - display scroll bar
    fn render_screen_normal_content(&mut self, area: Rect, buf: &mut Buffer) {
        let areas = self.split_area_per_tree(area, buf);
        let nb_trees = self.nb_trees();
        if let Some(context) = &mut self.context
            && context.list_panel.content_length > 0
        {
            context.content_panel.content_length = 10; // TODO: remove
            context.content_panel.normalize(area.height.into());

            let index = context.get_diff_entry_index_selected(self.view);

            for tree_index in 0..nb_trees {
                let (render_type, render_ctx) = if self.view.is_diff() {
                    context.get_render_type_mode_diff(index, tree_index)
                } else {
                    context.get_render_type_mode_sync(index, tree_index)
                };
                let (diff_base_style, diff_highlight_style) =
                    get_render_styles(&self.theme, render_type);

                let [metadata_area, _content_area] = Layout::vertical([
                    Constraint::Length(NB_METADATA_LINES as u16),
                    Constraint::Fill(1),
                ])
                .areas(areas[tree_index]);

                Self::render_screen_normal_content_metadata(
                    render_ctx,
                    diff_base_style,
                    diff_highlight_style,
                    tree_index,
                    metadata_area,
                    buf,
                );
            }
        }
    }

    fn render_screen_normal_content_metadata(
        render_ctx: &dyn DiffEntryContextRender,
        diff_base_style: Style,
        diff_highlight_style: Style,
        tree_index: usize,
        area: Rect,
        buf: &mut Buffer,
    ) {
        let metadata = render_ctx.get_metadata(tree_index);

        for (line, row) in std::iter::zip(metadata.iter(), area.rows()) {
            line_diff_style(line, diff_highlight_style)
                .style(diff_base_style)
                .centered()
                .render(row, buf);
        }
    }

    /// Split one area into sub-area, one for each tree
    ///
    /// Fill spacers with bar style
    fn split_area_per_tree(&self, area: Rect, buf: &mut Buffer) -> Rc<[Rect]> {
        let bar_style = self.theme.bar_style();
        let (areas, spacers) =
            Layout::horizontal(std::iter::repeat_n(Constraint::Fill(1), self.nb_trees()))
                .spacing(1)
                .split_with_spacers(area);

        // fill spacers with bar style
        for spacer in spacers.iter() {
            buf.set_style(*spacer, bar_style);
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
            .border_style(self.theme.help_border_style())
            .border_type(self.theme.help_border_type())
            .style(self.theme.content_style())
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
            .border_style(self.theme.confirm_exit_border_style())
            .border_type(self.theme.confirm_exit_border_type())
            .style(self.theme.content_style())
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
            Effect::Code => self.theme.help_key_stroke_style(),
            Effect::Highlight => self.theme.help_highlight_style(),
        };
        Span::styled(txt.text.as_str(), style)
    }

    /// Number of trees to be displayed
    pub fn nb_trees(&self) -> usize {
        self.arg.dirs.len()
    }
}

/// Get rendering styles based on render type
fn get_render_styles(theme: &AppTheme, render_type: RenderDiffType) -> (Style, Style) {
    match render_type {
        RenderDiffType::ConflictAlways | RenderDiffType::ConflictDiff => (
            theme.diff_content_conflict_base_style(),
            theme.diff_content_conflict_highlight_style_patch(),
        ),
        RenderDiffType::Old => (
            theme.diff_content_old_base_style(),
            theme.diff_content_old_highlight_style_patch(),
        ),
        RenderDiffType::New => (
            theme.diff_content_new_base_style(),
            theme.diff_content_new_highlight_style_patch(),
        ),
    }
}

/// Create `Span` with content, applying `highlight_style` when differs
fn span_diff_style(chunk: &DiffChunk, highlight_style: Style) -> Span<'_> {
    let mut span = Span::from(&chunk.s);
    if chunk.kind == DiffChunkType::Differ {
        span = span.style(highlight_style);
    }
    span
}

/// Create `Line` with content, applying `highlight_style` when differs
fn line_diff_style(line: &DiffLine, highlight_style: Style) -> Line<'_> {
    Line::from(
        line.0
            .iter()
            .map(|chunk| span_diff_style(chunk, highlight_style))
            .collect::<Vec<_>>(),
    )
}
