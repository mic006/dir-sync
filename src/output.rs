//! Output of `MetadataSnap`

use std::io::Write as _;

use crate::generic::format::hash::format_hash;
use crate::generic::format::owner::OwnerGroupDb;
use crate::generic::format::permissions::format_file_type_and_permissions;
use crate::generic::format::size::format_file_size;
use crate::generic::format::timestamp::{format_opt_ts, format_ts};
use crate::generic::format::tree::FormatTree;
use crate::proto::{MetadataSnap, MyDirEntry, Specific};

/// Output `MetadataSnap` to stdout
pub fn output(snap: &MetadataSnap) {
    let _ignored = Output::default().output_exit_on_error(snap);
}

struct Output {
    stdout: std::io::Stdout,
    owner_group_db: OwnerGroupDb,
    format_tree: FormatTree,
}

impl Default for Output {
    fn default() -> Self {
        Self {
            stdout: std::io::stdout(),
            owner_group_db: OwnerGroupDb::default(),
            format_tree: FormatTree::default(),
        }
    }
}

impl Output {
    /// Output `MetadataSnap` to stdout, with easy exit on error (stdout closed)
    fn output_exit_on_error(&mut self, snap: &MetadataSnap) -> anyhow::Result<()> {
        writeln!(self.stdout, "ts: {}", format_opt_ts(snap.ts.as_ref()))?;

        writeln!(self.stdout, "path: {}", snap.path)?;

        if snap.last_syncs.is_empty() {
            writeln!(self.stdout, "last_syncs: []")?;
        } else {
            writeln!(self.stdout, "last_syncs:")?;
            for (sync_path, sync_ts) in &snap.last_syncs {
                writeln!(self.stdout, "  - {}  {}", format_ts(sync_ts), sync_path)?;
            }
        }

        if let Some(root) = &snap.root {
            writeln!(self.stdout, "content:")?;
            self.output_entry(root, true)?;
        } else {
            writeln!(self.stdout, "content: None")?;
        }

        Ok(())
    }

    /// Output directory content, recursively
    fn output_dir(&mut self, entries: &[MyDirEntry]) -> anyhow::Result<()> {
        self.format_tree.entering_sub();
        for (i, entry) in entries.iter().enumerate() {
            let last_in_folder = i == entries.len() - 1;
            self.output_entry(entry, last_in_folder)?;
        }
        self.format_tree.leaving_sub();
        Ok(())
    }

    /// Output one entry, on one line
    fn output_entry(&mut self, entry: &MyDirEntry, last_in_folder: bool) -> anyhow::Result<()> {
        let file_perm = format_file_type_and_permissions(entry);
        let entry_sz = format_file_size(entry);
        let owner_group = self.owner_group_db.format_owner_group(entry);
        let ts = format_opt_ts(entry.mtime.as_ref());
        let hash_str = format_hash(entry);
        let tree = self.format_tree.entry(last_in_folder);
        writeln!(
            self.stdout,
            "{file_perm} {entry_sz} {owner_group} {ts} {hash_str} {tree}{}",
            entry.file_name
        )?;
        if let Some(Specific::Directory(dir_data)) = &entry.specific
            && !dir_data.content.is_empty()
        {
            self.output_dir(&dir_data.content)?;
        }
        Ok(())
    }
}
