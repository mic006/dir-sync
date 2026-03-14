//! Rendering of TUI application on terminal

use ratatui::widgets::Widget;

use super::App;

impl Widget for &App {
    fn render(self, _area: ratatui::prelude::Rect, _buf: &mut ratatui::prelude::Buffer)
    where
        Self: Sized,
    {
        // TODO
    }
}
