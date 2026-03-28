//! TUI application context for diff handling

use crate::diff::{self, DiffEntry};
use crate::generic::bitmap_categ::{BitmapCateg, Categ};
use crate::generic::format::owner::OwnerGroupDb;
use crate::generic::format::permissions::format_file_type_and_permissions;
use crate::generic::str_diff::{DiffChunkVec, diff_fixed_ascii_str};
use crate::proto::TimestampOrd as _;

use super::RunContext;
use super::list_panel::{ListPanel, ListPanelSelection};
use super::{InitContext, View};

/// How to render fields of one tree
pub enum RenderDiffType {
    /// All different fields (`DiffType`) rendered as conflict
    ConflictAlways,
    /// Compare fields, rendered as conflict
    ConflictDiff,
    /// Compare fields, rendered as old
    Old,
    /// Compare fields, rendered as new
    New,
}
impl RenderDiffType {
    pub fn render_diff(&self) -> bool {
        !matches!(self, RenderDiffType::ConflictAlways)
    }
}

/// Cache of permission delta
#[derive(Clone)]
struct PermissionDelta {
    /// Permission string for `Categ::ZERO`
    perm0: String,
    /// Permission string for `Categ::ONE`
    perm1: String,
    /// View to highlight differences between perm0 and perm1
    diff_view: DiffChunkVec,
}

/// When one `DiffEntry` can show comparison (2 different entries)
/// Cache values for comparison
#[derive(Clone)]
struct DiffEntryRenderDiffContext {
    /// The N trees contains 2 different entries; map them to `Categ::ZERO` or `Categ::ONE`
    /// In diff mode, `Categ::ZERO` is old and `Categ::ONE` is new
    /// In sync mode, it depends on the selected `sync_action`
    categ: BitmapCateg,
    /// Delta of permissions, if permissions are different
    perm_delta: Option<PermissionDelta>,
}

/// Difference applicable to the `DiffEntry`
#[derive(Debug, PartialEq, Clone, Copy)]
enum DiffEntryDiffType {
    /// More than 2 different entries: cannot compare, any delta is rendered as conflict
    ConflictAlways,
    /// Diff mode: 2 entries with same mtime, cannot determine old vs new
    Conflict,
    /// Regular diff mode
    /// Diff mode: mtime is used to identify new vs old
    /// Sync mode: `sync_action` is used to identify new vs old
    OldNewDiff,
}

/// Context related to one `DiffEntry`
#[derive(Clone)]
struct DiffEntryContext {
    /// Diff type
    typ: DiffEntryDiffType,
    /// Rendering context, applicable if `typ` is not `DiffEntryDiffType::ConflictAlways`
    render_ctx: Option<DiffEntryRenderDiffContext>,
    // TODO:: add content
}

/// Application runtime context (displaying diffs)
pub struct DiffContext {
    pub run_ctx: RunContext,
    pub owner_group_db: OwnerGroupDb,
    /// List of diffs, with sync action
    pub diffs: Vec<DiffEntry>,
    /// Diff list panel
    pub list_panel: ListPanelSelection,
    /// Content list panel
    pub content_panel: ListPanel,
    /// List of index of `diffs` entries which have no sync action
    conflicts_indexes: Vec<usize>,
    /// List of index of `diffs` entries which have a sync action
    resolved_indexes: Vec<usize>,
    /// Context for diff rendering, matching entries in `diffs`
    /// Items are determined on the first rendering
    entries_context: Vec<Option<DiffEntryContext>>,
}
impl DiffContext {
    pub fn new(ctx: InitContext) -> Self {
        let list_panel = ListPanelSelection::new(ctx.diffs.len());
        let content_panel = ListPanel::new(0); // TODO

        let mut conflicts_indexes = Vec::with_capacity(ctx.diffs.len());
        let mut resolved_indexes = Vec::with_capacity(ctx.diffs.len());

        for (i, diff) in ctx.diffs.iter().enumerate() {
            if diff.sync_source_index.is_none() {
                conflicts_indexes.push(i);
            } else {
                resolved_indexes.push(i);
            }
        }

        let entries_context = vec![None; ctx.diffs.len()];

        Self {
            run_ctx: ctx.run_ctx,
            owner_group_db: OwnerGroupDb::default(),
            diffs: ctx.diffs,
            list_panel,
            content_panel,
            conflicts_indexes,
            resolved_indexes,
            entries_context,
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
    pub fn get_diff_entry_index_selected(&self, view: View) -> usize {
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

    /// Get `RenderDiffType` for one tree in diff mode
    pub fn get_render_type_mode_diff(
        &mut self,
        diff_index: usize,
        tree_index: usize,
    ) -> RenderDiffType {
        if self.entries_context[diff_index].is_none() {
            self.gen_entry_context(diff_index, false);
        }
        let entry_ctx = self.entries_context[diff_index].as_ref().unwrap();
        match entry_ctx.typ {
            DiffEntryDiffType::ConflictAlways => RenderDiffType::ConflictAlways,
            DiffEntryDiffType::Conflict => RenderDiffType::ConflictDiff,
            DiffEntryDiffType::OldNewDiff => {
                match entry_ctx.render_ctx.as_ref().unwrap().categ.get(tree_index) {
                    Categ::ZERO => RenderDiffType::Old,
                    Categ::ONE => RenderDiffType::New,
                }
            }
        }
    }

    /// Get `RenderDiffType` for one tree in sync mode
    pub fn get_render_type_mode_sync(
        &mut self,
        diff_index: usize,
        tree_index: usize,
    ) -> RenderDiffType {
        if self.entries_context[diff_index].is_none() {
            self.gen_entry_context(diff_index, true);
        }
        let entry_ctx = self.entries_context[diff_index].as_ref().unwrap();
        match entry_ctx.typ {
            DiffEntryDiffType::ConflictAlways => RenderDiffType::ConflictAlways,
            DiffEntryDiffType::Conflict => unreachable!("unused state in sync mode"),
            DiffEntryDiffType::OldNewDiff => {
                let diff_entry = &self.diffs[diff_index];
                if let Some(sync_source_index) = diff_entry.sync_source_index {
                    let categ = &entry_ctx.render_ctx.as_ref().unwrap().categ;
                    if sync_source_index == tree_index as u8
                        || categ.get(tree_index) == categ.get(sync_source_index as usize)
                    {
                        // tree is source_index or has same content as source_index
                        RenderDiffType::New
                    } else {
                        RenderDiffType::Old
                    }
                } else {
                    RenderDiffType::ConflictDiff
                }
            }
        }
    }

    fn gen_entry_context(&mut self, diff_index: usize, sync_mode: bool) {
        let diff_entry = &self.diffs[diff_index];
        let mut categ = BitmapCateg::default();
        let mut categ_zero = diff_entry.entries[0].as_ref();
        let mut categ_one = None;
        let mut more_than_two_entries = false;

        // determine and classify the entry from each tree
        for (i, entry) in diff_entry.entries.iter().enumerate().skip(1) {
            if !diff::diff_entries_opt(entry.as_ref(), categ_zero).is_empty() {
                if let Some(categ_one) = categ_one {
                    if !diff::diff_entries_opt(entry.as_ref(), categ_one).is_empty() {
                        more_than_two_entries = true;
                        break;
                    }
                    // matching categ_one
                } else {
                    // defines categ_one
                    categ_one = Some(entry.as_ref());
                }
                categ.set(i, Categ::ONE);
            }
        }

        if more_than_two_entries {
            self.entries_context[diff_index] = Some(DiffEntryContext {
                typ: DiffEntryDiffType::ConflictAlways,
                render_ctx: None,
            });
            return;
        }

        let mut categ_one = categ_one.expect("diff entry shall have 2 different entries");

        let typ = if sync_mode {
            DiffEntryDiffType::OldNewDiff
        } else {
            // try to identify old vs new
            if let Some(categ_zero_entry) = categ_zero {
                if let Some(categ_one_entry) = categ_one {
                    // two existing files, compare mtime
                    match categ_zero_entry.mtime.cmp(&categ_one_entry.mtime) {
                        std::cmp::Ordering::Less => DiffEntryDiffType::OldNewDiff,
                        std::cmp::Ordering::Equal => {
                            // two files with same mtime => conflict
                            DiffEntryDiffType::Conflict
                        }
                        std::cmp::Ordering::Greater => {
                            // categ::ZERO shall be old content, categ::ONE shall be new content
                            categ.revert(self.nb_trees());
                            std::mem::swap(&mut categ_zero, &mut categ_one);
                            DiffEntryDiffType::OldNewDiff
                        }
                    }
                } else {
                    // one existing file => consider new
                    // categ::ZERO shall be old content, categ::ONE shall be new content
                    categ.revert(self.nb_trees());
                    std::mem::swap(&mut categ_zero, &mut categ_one);
                    DiffEntryDiffType::OldNewDiff
                }
            } else {
                // one existing file => consider new
                DiffEntryDiffType::OldNewDiff
            }
        };

        let perm_delta = if diff_entry.diff.contains(diff::DiffType::PERMISSIONS) {
            let perm0 = format_file_type_and_permissions(categ_zero.as_ref().unwrap());
            let perm1 = format_file_type_and_permissions(categ_one.as_ref().unwrap());
            let diff_view = diff_fixed_ascii_str(&perm0, &perm1);
            Some(PermissionDelta {
                perm0,
                perm1,
                diff_view,
            })
        } else {
            None
        };

        self.entries_context[diff_index] = Some(DiffEntryContext {
            typ,
            render_ctx: Some(DiffEntryRenderDiffContext { categ, perm_delta }),
        });
    }

    fn nb_trees(&self) -> usize {
        self.run_ctx.trees.len()
    }
}
