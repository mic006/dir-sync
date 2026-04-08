//! Helper to manage a panel displaying a list

use std::ops::Range;

use super::scroll_bar::ScrollBar;

/// Movement event
pub enum ListPanelMove {
    Start,
    End,
    PageUp,
    PageDown,
    LineUp,
    LineDown,
}

/// Panel displaying a list, without selection
pub struct ListPanel {
    /// Index of first visible item
    pub view_start: usize,
    /// Number of visible items
    pub view_length: usize,
    /// Number of items in the list
    pub content_length: usize,
}

impl ListPanel {
    /// Create a new panel
    pub fn new(content_length: usize) -> Self {
        Self {
            view_start: 0,
            view_length: 0,
            content_length,
        }
    }

    /// Reset panel with new content length
    pub fn reset(&mut self, content_length: usize) {
        self.view_start = 0;
        self.content_length = content_length;
    }

    /// Update based on move event
    pub fn handle(&mut self, event: ListPanelMove) {
        match event {
            ListPanelMove::Start => {
                self.view_start = 0;
            }
            ListPanelMove::End => {
                self.view_start = self.view_start_max();
            }
            ListPanelMove::PageUp => {
                self.view_start = self.view_start.saturating_sub(self.view_length);
            }
            ListPanelMove::PageDown => {
                self.view_start = (self.view_start + self.view_length).min(self.view_start_max());
            }
            ListPanelMove::LineUp => {
                self.view_start = self.view_start.saturating_sub(1);
            }
            ListPanelMove::LineDown => {
                self.view_start = (self.view_start + 1).min(self.view_start_max());
            }
        }
    }

    /// Adjust view to new `view_length`
    pub fn normalize(&mut self, view_length: usize) {
        if view_length == self.view_length {
            return;
        }
        self.view_length = view_length;
        self.view_start = self.view_start.min(self.view_start_max());
    }

    /// Get range of visible items
    pub fn view_range(&self) -> Range<usize> {
        self.view_start..(self.view_start + self.view_length).min(self.content_length)
    }

    /// Get scroll bar for the panel, if needed
    pub fn scroll_bar(&self) -> Option<ScrollBar> {
        ScrollBar::new(self.view_start, self.view_length, self.content_length)
    }

    /// Get max value of `view_start`
    fn view_start_max(&self) -> usize {
        self.content_length.saturating_sub(self.view_length)
    }
}

/// Panel displaying a list, with a selected item
pub struct ListPanelSelection {
    /// Selected item
    pub selected: usize,
    /// Index of first visible item
    view_start: usize,
    /// Number of visible items
    view_length: usize,
    /// Number of items in the list
    pub content_length: usize,
}

impl ListPanelSelection {
    /// Create a new panel
    pub fn new(content_length: usize) -> Self {
        Self {
            selected: 0,
            view_start: 0,
            view_length: 0,
            content_length,
        }
    }

    /// Reset panel with new content length
    pub fn reset(&mut self, content_length: usize) {
        self.selected = 0;
        self.view_start = 0;
        self.content_length = content_length;
    }

    /// Adjust content length, and ensure selected remains valid
    ///
    /// Note: `normalize()` shall be called afterwards to ensure the selected item is within the view
    pub fn adjust_content_length(&mut self, content_length: usize) {
        self.content_length = content_length;
        self.selected = self.selected.min(self.content_length.saturating_sub(1));
    }

    /// Update based on move event
    ///
    /// Note: `normalize()` shall be called afterwards to ensure the selected item is within the view
    pub fn handle(&mut self, event: ListPanelMove) {
        match event {
            ListPanelMove::Start => {
                self.selected = 0;
                // view_start will be adjusted by normalize()
            }
            ListPanelMove::End => {
                self.selected = self.selected_max();
                // view_start will be adjusted by normalize()
            }
            ListPanelMove::PageUp => {
                self.selected = self.selected.saturating_sub(self.view_length);
                self.view_start = self.view_start.saturating_sub(self.view_length);
            }
            ListPanelMove::PageDown => {
                self.selected = (self.selected + self.view_length).min(self.selected_max());
                self.view_start = (self.view_start + self.view_length).min(self.view_start_max());
            }
            ListPanelMove::LineUp => {
                self.selected = self.selected.saturating_sub(1);
                // view_start will be adjusted by normalize() if needed
            }
            ListPanelMove::LineDown => {
                self.selected = (self.selected + 1).min(self.selected_max());
                // view_start will be adjusted by normalize() if needed
            }
        }
    }

    /// Adjust view to new `view_length` and ensure the selected item is within the view
    pub fn normalize(&mut self, view_length: usize) {
        self.view_length = view_length;

        // ensure the selected item is within the view
        if self.selected == 0 {
            self.view_start = 0;
        } else if self.selected == self.selected_max() {
            self.view_start = self.view_start_max();
        } else {
            self.view_start = self
                .view_start
                .max((self.selected + 2).saturating_sub(self.view_length))
                .min(self.selected - 1)
                .min(self.view_start_max());
        }
    }

    /// Get range of visible items
    pub fn view_range(&self) -> Range<usize> {
        self.view_start..(self.view_start + self.view_length).min(self.content_length)
    }

    /// Get scroll bar for the panel, if needed
    pub fn scroll_bar(&self) -> Option<ScrollBar> {
        ScrollBar::new(self.view_start, self.view_length, self.content_length)
    }

    /// Get max value of `selected`
    fn selected_max(&self) -> usize {
        self.content_length.saturating_sub(1)
    }

    /// Get max value of `view_start`
    fn view_start_max(&self) -> usize {
        self.content_length.saturating_sub(self.view_length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_panel_basics() {
        let mut panel = ListPanel::new(100);
        panel.normalize(10);
        assert_eq!(panel.view_start, 0);
        assert_eq!(panel.view_length, 10);
        assert_eq!(panel.content_length, 100);

        // Start -> no change
        panel.handle(ListPanelMove::Start);
        assert_eq!(panel.view_start, 0);

        // LineDown
        panel.handle(ListPanelMove::LineDown);
        assert_eq!(panel.view_start, 1);

        // PageDown -> +10 -> 11
        panel.handle(ListPanelMove::PageDown);
        assert_eq!(panel.view_start, 11);

        // End -> 90
        panel.handle(ListPanelMove::End);
        assert_eq!(panel.view_start, 90);

        // LineDown -> clamped
        panel.handle(ListPanelMove::LineDown);
        assert_eq!(panel.view_start, 90);

        // PageUp -> -10 -> 80
        panel.handle(ListPanelMove::PageUp);
        assert_eq!(panel.view_start, 80);

        // LineUp -> 79
        panel.handle(ListPanelMove::LineUp);
        assert_eq!(panel.view_start, 79);
    }

    #[test]
    fn test_list_panel_resize() {
        let mut panel = ListPanel::new(100);
        panel.view_start = 90;
        panel.view_length = 10;

        // increase view length -> view_start should decrease to fit
        panel.normalize(20);
        // max start = 100 - 20 = 80
        assert_eq!(panel.view_start, 80);

        // decrease view length -> view_start remains if valid
        panel.view_start = 10;
        panel.normalize(5);
        assert_eq!(panel.view_start, 10);
    }

    #[test]
    fn test_list_panel_selection_basics() {
        let mut panel = ListPanelSelection::new(100);
        panel.normalize(10);
        assert_eq!(panel.selected, 0);
        assert_eq!(panel.view_start, 0);

        // LineDown -> selected 1
        // normalize: scrolloff 1 top -> view_start 0
        panel.handle(ListPanelMove::LineDown);
        panel.normalize(10);
        assert_eq!(panel.selected, 1);
        assert_eq!(panel.view_start, 0);

        // PageDown -> selected 11, view_start += 10 -> 10
        // normalize:
        // selected 11.
        // view_start 10.
        // range [10, 20). 11 is at index 1 relative to view.
        // check top scrolloff: view_start <= selected - 1 = 10. OK.
        panel.handle(ListPanelMove::PageDown);
        panel.normalize(10);
        assert_eq!(panel.selected, 11);
        assert_eq!(panel.view_start, 10);

        // End -> selected 99, view_start adjusted
        panel.handle(ListPanelMove::End);
        panel.normalize(10);
        assert_eq!(panel.selected, 99);
        assert_eq!(panel.view_start, 90);

        // PageUp -> selected 89, view_start 80
        // normalize:
        // selected 89.
        // view_start 81 so that selected is not the last displayed item
        panel.handle(ListPanelMove::PageUp);
        panel.normalize(10);
        assert_eq!(panel.selected, 89);
        assert_eq!(panel.view_start, 81);
    }

    #[test]
    fn test_list_panel_reset() {
        let mut panel = ListPanel::new(50);
        panel.view_start = 10;
        panel.reset(20);
        assert_eq!(panel.view_start, 0);
        assert_eq!(panel.content_length, 20);
    }
}
