//! TUI application context for diff handling

use crate::diff::DiffEntry;

use super::RunContext;
use super::list_panel::ListPanelSelection;
use super::{InitContext, View};

enum RenderDiffType {
    Conflict,
    Old,
    New,
}

/// Application runtime context (displaying diffs)
pub struct DiffContext {
    pub run_ctx: RunContext,
    /// List of diffs, with sync action
    pub diffs: Vec<DiffEntry>,
    /// Diff list panel
    pub list_panel: ListPanelSelection,
    /// List of index of `diffs` entries which have no sync action
    conflicts_indexes: Vec<usize>,
    /// List of index of `diffs` entries which have a sync action
    resolved_indexes: Vec<usize>,
}
impl DiffContext {
    pub fn new(ctx: InitContext) -> Self {
        let list_panel = ListPanelSelection::new(ctx.diffs.len());
        let mut conflicts_indexes = Vec::with_capacity(ctx.diffs.len());
        let mut resolved_indexes = Vec::with_capacity(ctx.diffs.len());

        for (i, diff) in ctx.diffs.iter().enumerate() {
            if diff.sync_source_index.is_none() {
                conflicts_indexes.push(i);
            } else {
                resolved_indexes.push(i);
            }
        }

        Self {
            run_ctx: ctx.run_ctx,
            diffs: ctx.diffs,
            list_panel,
            conflicts_indexes,
            resolved_indexes,
        }
    }

    pub fn set_view(&mut self, view: View) {
        match view {
            View::Diff | View::SyncAll => {
                self.list_panel.reset(self.diffs.len());
            }
            View::SyncConflicts => {
                self.list_panel.reset(self.conflicts_indexes.len());
            }
            View::SyncResolved => {
                self.list_panel.reset(self.resolved_indexes.len());
            }
            _ => unreachable!("context is invalid for the unhandled views"),
        }
    }

    /// Get diff entry index for current view
    pub fn get_diff_entry_index(&self, view: View, view_index: usize) -> usize {
        match view {
            View::Diff | View::SyncAll => view_index,
            View::SyncConflicts => self.conflicts_indexes[view_index],
            View::SyncResolved => self.resolved_indexes[view_index],
            _ => unreachable!("context is invalid for the unhandled views"),
        }
    }

    /// Get diff entry index for selected item in current view
    fn get_diff_entry_index_selected(&self, view: View) -> usize {
        let view_index = self.list_panel.selected;
        self.get_diff_entry_index(view, view_index)
    }

    /// Adjust selected item in list panel if needed
    fn adjust_selected(&mut self, view: View) {
        let list = match view {
            View::Diff | View::SyncAll => return,
            View::SyncConflicts => &self.conflicts_indexes,
            View::SyncResolved => &self.resolved_indexes,
            _ => unreachable!("context is invalid for the unhandled views"),
        };
        self.list_panel.adjust_content_length(list.len());
    }

    /// Manage digit keys to choose sync action
    pub fn handle_key_sync_action(&mut self, key: usize, view: View) {
        let index = self.get_diff_entry_index_selected(view);
        let diff_entry = &mut self.diffs[index];

        if diff_entry.sync_source_index.is_none() {
            if key != 0 {
                // defining sync action
                diff_entry.sync_source_index = Some((key - 1) as u8);
                // update lists
                Self::remove_index(&mut self.conflicts_indexes, index);
                Self::add_index(&mut self.resolved_indexes, index);
                self.adjust_selected(view);
            }
            // else: no sync action, nothing to do
        } else if key == 0 {
            // removing sync action
            diff_entry.sync_source_index = None;
            Self::add_index(&mut self.conflicts_indexes, index);
            Self::remove_index(&mut self.resolved_indexes, index);
            // update lists
            self.adjust_selected(view);
        } else {
            // modifying the sync action
            diff_entry.sync_source_index = Some((key - 1) as u8);
        }
    }

    /// Remove index from ordered list
    fn remove_index(list: &mut Vec<usize>, index: usize) {
        let idx = list.binary_search(&index).unwrap();
        list.remove(idx);
    }

    /// Add index to ordered list
    fn add_index(list: &mut Vec<usize>, index: usize) {
        let idx = list.binary_search(&index).unwrap_err();
        list.insert(idx, index);
    }
}
