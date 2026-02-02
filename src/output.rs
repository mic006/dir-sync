//! Output of `MetadataSnap`

use std::io::Write as _;

use prost_types::Timestamp;

use crate::generic::format::timestamp::format_ts;
use crate::proto::MetadataSnap;

/// Output `MetadataSnap` to stdout
pub fn output(snap: &MetadataSnap) {
    let _ignored = output_exit_on_error(snap);
}

/// Get timestamp in human format
fn get_ts(ts: Option<&Timestamp>) -> String {
    ts.map_or_else(|| String::from("N/A"), format_ts)
}

fn output_exit_on_error(snap: &MetadataSnap) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout();

    writeln!(stdout, "ts: {}", get_ts(snap.ts.as_ref()))?;

    writeln!(stdout, "path: {}", snap.path)?;

    if snap.last_syncs.is_empty() {
        writeln!(stdout, "last_syncs: []")?;
    } else {
        writeln!(stdout, "last_syncs:")?;
        for sync_status in &snap.last_syncs {
            writeln!(
                stdout,
                "  - {}  {}",
                get_ts(sync_status.ts.as_ref()),
                sync_status.sync_path
            )?;
        }
    }

    if let Some(_root) = &snap.root {
        todo!();
    } else {
        writeln!(stdout, "root: None")?;
    }

    Ok(())
}
