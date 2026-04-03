//! TUI application context for diff handling

use std::sync::Arc;

use crate::config::TuiConfigRef;
use crate::diff::{self, DiffEntry, DiffType};
use crate::generic::bitmap_categ::{BitmapCateg, Categ};
use crate::generic::file::MyHash;
use crate::generic::format::owner::OwnerGroupDb;
use crate::generic::format::permissions::format_file_type_and_permissions;
use crate::generic::format::size::format_file_size;
use crate::generic::format::timestamp::format_opt_ts;
use crate::generic::str_diff::{DiffChunkType, DiffLine, DiffMultiline, diff_fixed_ascii_str};
use crate::proto::action::FileReadReq;
use crate::proto::{ActionReq, MyDirEntryExt as _, Specific, TimestampOrd as _};

use super::RunContext;
use super::list_panel::{ListPanel, ListPanelMove, ListPanelSelection};
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

/// State for content of one file
pub enum ContentState {
    /// No content: directory, empty file
    Empty,
    /// Content requested to tree, waiting for response
    Waiting,
    /// String content, ready for comparison with counterpart (`DiffEntryContextDiff`)
    Str(String),
    /// Content ready for rendering, single line (symlink)
    RenderSingle(String),
    /// Content ready for rendering, multi lines (file content)
    RenderMulti(DiffMultiline),
    /// Content is too big, not retrieved nor displayed. Give file hash as hex string
    TooBig(String),
    /// Binary content, not displayed. Give file hash as hex string
    Binary(String),
    /// All trees have the same content
    SameContent,
}

const METADATA_LINE_TYPE_SIZE: usize = 0;
const METADATA_LINE_MTIME: usize = 1;
const METADATA_LINE_PERMISSIONS_OWNER_GROUP: usize = 2;
pub const NB_METADATA_LINES: usize = 3;

type MetadataLines = [DiffLine; NB_METADATA_LINES];

pub trait DiffEntryContextRender {
    fn get_metadata(&self, tree_index: usize) -> &MetadataLines;
    fn get_content(&self, tree_index: usize) -> &ContentState;

    fn set_raw_content(&mut self, tree_index: usize, content: Vec<u8>);
}

/// When one `DiffEntry` can show comparison (2 different entries)
struct DiffEntryContextDiff {
    /// The N trees contains 2 different entries; map them to `Categ::ZERO` or `Categ::ONE`
    /// In diff mode, `Categ::ZERO` is old and `Categ::ONE` is new
    /// In sync mode, it depends on the selected `sync_action`
    categ: BitmapCateg,
    /// Metadata lines for each tree category
    metadata: [MetadataLines; 2],
    /// Content for each tree category; None means same content for all trees
    content: Option<[ContentState; 2]>,
}
impl DiffEntryContextRender for DiffEntryContextDiff {
    fn get_metadata(&self, tree_index: usize) -> &MetadataLines {
        &self.metadata[self.categ.get(tree_index) as usize]
    }

    fn get_content(&self, tree_index: usize) -> &ContentState {
        self.content
            .as_ref()
            .map_or(&ContentState::SameContent, |content| {
                &content[self.categ.get(tree_index) as usize]
            })
    }

    fn set_raw_content(&mut self, _tree_index: usize, _content: Vec<u8>) {
        todo!();
    }
}

/// When one `DiffEntry` cannot show comparison (3+ entries)
struct DiffEntryContextConflict {
    /// Metadata lines for each tree
    metadata: Vec<MetadataLines>,
    /// Content for each tree; None means same content for all trees
    content: Option<Vec<ContentState>>,
}
impl DiffEntryContextRender for DiffEntryContextConflict {
    fn get_metadata(&self, tree_index: usize) -> &MetadataLines {
        &self.metadata[tree_index]
    }

    fn get_content(&self, tree_index: usize) -> &ContentState {
        self.content
            .as_ref()
            .map_or(&ContentState::SameContent, |content| &content[tree_index])
    }

    fn set_raw_content(&mut self, _tree_index: usize, _content: Vec<u8>) {
        todo!();
    }
}

/// Bare enum aligned with `DiffEntryContext` to simplify building
enum DiffEntryContextType {
    Conflict,
    OldNewDiff,
}

/// Context related to one `DiffEntry`
#[derive(Default)]
enum DiffEntryContext {
    /// Item not determined yet
    #[default]
    None,
    /// More than 2 different entries: cannot compare, any delta is rendered as conflict
    ConflictAlways(DiffEntryContextConflict),
    /// Diff mode: 2 entries with same mtime, cannot determine old vs new
    Conflict(DiffEntryContextDiff),
    /// Regular diff mode, old vs new
    /// Diff mode: mtime is used to identify new vs old
    /// Sync mode: `sync_action` is used to identify new vs old, when no `sync_action` it will be rendered as conflict
    OldNewDiff(DiffEntryContextDiff),
}
impl DiffEntryContext {
    pub fn is_none(&self) -> bool {
        matches!(self, DiffEntryContext::None)
    }
}

/// Application runtime context (displaying diffs)
pub struct DiffContext {
    pub run_ctx: RunContext,
    pub config: TuiConfigRef,
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
    entries_context: Vec<DiffEntryContext>,
}
impl DiffContext {
    pub fn new(ctx: InitContext, config: TuiConfigRef) -> Self {
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

        let mut entries_context = Vec::new();
        entries_context.resize_with(ctx.diffs.len(), Default::default);

        Self {
            run_ctx: ctx.run_ctx,
            config,
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
        self.on_selection_update(view);
    }

    /// Update list panel with given event
    ///
    /// - manage panel update
    /// - manage selection change: prepare content view for the newly selected item if needed
    pub fn update_list_panel(&mut self, view: View, event: ListPanelMove) {
        self.list_panel.handle(event);
        self.on_selection_update(view);
    }

    /// Update content panel with new selection
    fn on_selection_update(&mut self, view: View) {
        let diff_index = self.get_diff_entry_index_selected(view);
        if diff_index < self.diffs.len() && self.entries_context[diff_index].is_none() {
            self.gen_entry_context(diff_index, !view.is_diff());
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
        let view_index: usize = self.list_panel.selected;
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

    /// Get rendering info for one element
    pub fn get_content_renderer(
        &mut self,
        tree_index: usize,
        view: View,
    ) -> (RenderDiffType, &dyn DiffEntryContextRender) {
        let diff_index = self.get_diff_entry_index_selected(view);
        assert!(diff_index < self.diffs.len());
        match &self.entries_context[diff_index] {
            DiffEntryContext::None => {
                unreachable!("element shall be created by 'on_selection_update()'")
            }
            DiffEntryContext::ConflictAlways(ctx) => (RenderDiffType::ConflictAlways, ctx),
            DiffEntryContext::Conflict(ctx) => (RenderDiffType::ConflictDiff, ctx),
            DiffEntryContext::OldNewDiff(ctx) => {
                if view.is_diff() {
                    // diff mode: Categ::ZERO is old, Categ::ONE is new
                    match ctx.categ.get(tree_index) {
                        Categ::ZERO => (RenderDiffType::Old, ctx),
                        Categ::ONE => (RenderDiffType::New, ctx),
                    }
                } else {
                    let diff_entry = &self.diffs[diff_index];
                    if let Some(sync_source_index) = diff_entry.sync_source_index {
                        let categ = &ctx.categ;
                        if sync_source_index == tree_index as u8
                            || categ.get(tree_index) == categ.get(sync_source_index as usize)
                        {
                            // tree is source_index or has same content as source_index => new
                            (RenderDiffType::New, ctx)
                        } else {
                            // other content => old
                            (RenderDiffType::Old, ctx)
                        }
                    } else {
                        // no sync action => conflict
                        (RenderDiffType::ConflictDiff, ctx)
                    }
                }
            }
        }
    }

    /// Generate `DiffEntryContext` for one `DiffEntry`
    ///
    /// - evaluate which variant of `DiffEntryContext` shall be used
    /// - instantiate it
    fn gen_entry_context(&mut self, diff_index: usize, sync_mode: bool) {
        let diff_entry = &self.diffs[diff_index];
        let mut categ = BitmapCateg::default();
        let categ_zero = diff_entry.entries[0].as_ref();
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
            self.entries_context[diff_index] =
                DiffEntryContext::ConflictAlways(self.gen_entry_context_conflict(diff_index));
            return;
        }

        let categ_one = categ_one.expect("diff entry shall have 2 different entries");

        let typ = if sync_mode {
            DiffEntryContextType::OldNewDiff
        } else {
            // try to identify old vs new
            if let Some(categ_zero_entry) = categ_zero {
                if let Some(categ_one_entry) = categ_one {
                    // two existing files, compare mtime
                    match categ_zero_entry.mtime.cmp(&categ_one_entry.mtime) {
                        std::cmp::Ordering::Less => DiffEntryContextType::OldNewDiff,
                        std::cmp::Ordering::Equal => {
                            // two files with same mtime => conflict
                            DiffEntryContextType::Conflict
                        }
                        std::cmp::Ordering::Greater => {
                            // categ::ZERO shall be old content, categ::ONE shall be new content
                            categ.revert(self.nb_trees());
                            DiffEntryContextType::OldNewDiff
                        }
                    }
                } else {
                    // one existing file => consider new
                    // categ::ZERO shall be old content, categ::ONE shall be new content
                    categ.revert(self.nb_trees());
                    DiffEntryContextType::OldNewDiff
                }
            } else {
                // one existing file => consider new
                DiffEntryContextType::OldNewDiff
            }
        };

        let ctx = self.gen_entry_context_diff(diff_index, categ);
        self.entries_context[diff_index] = match typ {
            DiffEntryContextType::Conflict => DiffEntryContext::Conflict(ctx),
            DiffEntryContextType::OldNewDiff => DiffEntryContext::OldNewDiff(ctx),
        };
    }

    fn gen_entry_context_conflict(&mut self, diff_index: usize) -> DiffEntryContextConflict {
        let metadata = (0..self.nb_trees())
            .map(|tree_index| self.gen_entry_context_metadata(diff_index, tree_index, None))
            .collect::<Vec<_>>();

        DiffEntryContextConflict {
            metadata,
            content: None,
        }
    }

    fn gen_entry_context_diff(
        &mut self,
        diff_index: usize,
        categ: BitmapCateg,
    ) -> DiffEntryContextDiff {
        let categ0_idx = categ.get_first(Categ::ZERO);
        let categ1_idx = categ.get_first(Categ::ONE);
        let diff_entry = &self.diffs[diff_index];

        let [perm0, perm1] = if diff_entry.diff.contains(diff::DiffType::PERMISSIONS) {
            let perm0 =
                format_file_type_and_permissions(diff_entry.entries[categ0_idx].as_ref().unwrap());
            let perm1 =
                format_file_type_and_permissions(diff_entry.entries[categ1_idx].as_ref().unwrap());
            let (perm0, perm1) = diff_fixed_ascii_str(&perm0, &perm1);
            [Some(perm0), Some(perm1)]
        } else {
            [None, None]
        };

        let metadata = [(categ0_idx, perm0), (categ1_idx, perm1)].map(|(tree_index, perm)| {
            self.gen_entry_context_metadata(diff_index, tree_index, perm)
        });

        DiffEntryContextDiff {
            categ,
            metadata,
            content: None,
        }
    }

    /// Generate metadata for one tree
    fn gen_entry_context_metadata(
        &mut self,
        diff_index: usize,
        tree_index: usize,
        perm: Option<DiffLine>, // pre-built permission part
    ) -> MetadataLines {
        let mut metadata = MetadataLines::default();
        let diff_entry = &self.diffs[diff_index];
        let entry = diff_entry.entries[tree_index].as_ref();
        if let Some(entry) = entry {
            metadata[METADATA_LINE_TYPE_SIZE].append_str(
                diff_entry.diff.contains(DiffType::TYPE).into(),
                entry.type_as_str(),
            );
            if entry.is_file() {
                metadata[METADATA_LINE_TYPE_SIZE].append_str(DiffChunkType::Common, "  ");
                metadata[METADATA_LINE_TYPE_SIZE].append_str(
                    diff_entry.diff.contains(DiffType::CONTENT).into(),
                    &format_file_size(entry),
                );
            }

            metadata[METADATA_LINE_MTIME].append_str(
                diff_entry.diff.contains(DiffType::MTIME).into(),
                &format_opt_ts(entry.mtime.as_ref()),
            );

            if let Some(perm) = perm {
                // use pre-built permission part
                metadata[METADATA_LINE_PERMISSIONS_OWNER_GROUP] = perm;
            } else {
                // build permission part from entry
                metadata[METADATA_LINE_PERMISSIONS_OWNER_GROUP].append_str(
                    diff_entry.diff.contains(DiffType::PERMISSIONS).into(),
                    &format_file_type_and_permissions(entry),
                );
            }
            metadata[METADATA_LINE_PERMISSIONS_OWNER_GROUP].append_str(DiffChunkType::Common, "  ");
            metadata[METADATA_LINE_PERMISSIONS_OWNER_GROUP].append_str(
                diff_entry.diff.contains(DiffType::OWNER).into(),
                self.owner_group_db.get_owner(entry.uid),
            );
            metadata[METADATA_LINE_PERMISSIONS_OWNER_GROUP].append_chr(DiffChunkType::Common, ':');
            metadata[METADATA_LINE_PERMISSIONS_OWNER_GROUP].append_str(
                diff_entry.diff.contains(DiffType::GROUP).into(),
                self.owner_group_db.get_group(entry.gid),
            );
        } else {
            // no file is always a diff, the file exists in another tree
            metadata[METADATA_LINE_TYPE_SIZE].append_str(DiffChunkType::Differ, "No file");
            metadata[METADATA_LINE_MTIME].append_chr(DiffChunkType::Common, ' ');
            metadata[METADATA_LINE_PERMISSIONS_OWNER_GROUP].append_chr(DiffChunkType::Common, ' ');
        }
        metadata
    }

    /// Generate content for one tree
    fn gen_entry_context_content(&mut self, diff_index: usize, tree_index: usize) -> ContentState {
        let diff_entry = &self.diffs[diff_index];
        let entry = diff_entry.entries[tree_index].as_ref();
        if let Some(entry) = entry {
            match entry.specific.as_ref().unwrap() {
                Specific::Regular(regular_data) => {
                    if regular_data.size == 0 {
                        // empty file
                        ContentState::Empty
                    } else if regular_data.size
                        >= self.config.tui.content_max_size.as_nb_bytes() as u64
                    {
                        // file too big
                        ContentState::TooBig(hash_to_string(&regular_data.hash))
                    } else {
                        // ask for file content
                        let _ignored = self.run_ctx.trees[tree_index]
                            .get_fs_action_requester()
                            .send(Arc::new(ActionReq::ReadFile(FileReadReq {
                                rel_path: diff_entry.rel_path.clone(),
                            })));
                        // wait for response
                        ContentState::Waiting
                    }
                }
                Specific::Symlink(target) => ContentState::RenderSingle(target.clone()),
                _ => ContentState::Empty,
            }
        } else {
            // no entry
            ContentState::Empty
        }
    }

    fn nb_trees(&self) -> usize {
        self.run_ctx.trees.len()
    }
}

/// Convert raw content data to string
///
/// - check UTF8 validity
/// - replace tab with arrow
/// - detect other control code characters
///
/// # Errors
/// - invalid UTF8 (binary data)
/// - control code characters found
fn raw_content_to_content_state(raw: Vec<u8>) -> Result<String, ()> {
    // convert to string
    let s = String::from_utf8(raw).map_err(|_| ())?;

    // check control code characters
    s.chars()
        .map(|c| match c {
            '\t' => Ok('⭲'),
            '\n' => Ok('\n'),
            _ if c.is_control() => Err(()), // control character, not allowed
            _ => Ok(c),
        })
        .collect()
}

/// Convert file hash to string
fn hash_to_string(h: &[u8]) -> String {
    MyHash::from_slice(h).unwrap().to_hex().to_string()
}
