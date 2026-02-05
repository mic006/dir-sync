//! Manipulation of `MetadataSnap`

use std::path::PathBuf;

use crate::config::ConfigRef;
use crate::generic::fs::MessageExt as _;
use crate::proto::MetadataSnap;

/// Basename for main snapshot file
const SNAP_MAIN_BASENAME: &str = "latest";

/// Basename for temporary snapshot file, before overwriting main snapshot file
const SNAP_TEMP_BASENAME: &str = "latest_tmp";

/// Extension for snapshot files
const SNAP_EXT: &str = "snap.pb.bin.zst";

/// Read/write access to metadata snapshots
pub struct SnapAccess {
    /// Folder where metadata snapshots are stored
    folder_path: PathBuf,
}

impl SnapAccess {
    /// Create new instance
    #[must_use]
    pub fn new(cfg: &ConfigRef, canon_input_path: &str) -> Self {
        let folder_path = cfg
            .local_metadata_snap_path_user
            .join(Self::path_to_unique_id(canon_input_path));
        Self { folder_path }
    }

    /// Convert path to unique identifier using blake3 hash
    fn path_to_unique_id(path: &str) -> String {
        let hash = blake3::hash(path.as_bytes()).to_hex();
        let hash = &hash.as_str()[..32];
        hash.to_string()
    }

    /// Determine path of snapshot file
    fn snap_path(&self, basename: &str) -> PathBuf {
        self.folder_path.join(basename).with_extension(SNAP_EXT)
    }

    /// Load main metadata snapshot, if available
    #[must_use]
    pub fn load_main_snap(&self) -> Option<MetadataSnap> {
        let snap_path = self.snap_path(SNAP_MAIN_BASENAME);
        MetadataSnap::load_from_file(&snap_path).ok()
    }

    /// Load sync metadata snapshot, if available
    #[must_use]
    pub fn load_sync_snap(&self, sync_path: &str) -> Option<MetadataSnap> {
        let snap_path = self.snap_path(&Self::path_to_unique_id(sync_path));
        MetadataSnap::load_from_file(&snap_path).ok()
    }

    /// Save metadata snapshot + create links for synced remotes
    ///
    /// # Errors
    /// - cannot create snapshot destination folder
    /// - cannot save main snapshot
    /// - cannot save sync snapshots
    pub fn save_snap(&self, snap: &MetadataSnap, synced_remotes: &[&str]) -> anyhow::Result<()> {
        if !self.folder_path.exists() {
            std::fs::create_dir(&self.folder_path).map_err(|err| {
                anyhow::anyhow!(
                    "cannot create snapshot destination folder {}: {err}",
                    self.folder_path.display(),
                )
            })?;
        }

        // create main snapshot file
        let main_snap_path = self.snap_path(SNAP_MAIN_BASENAME);
        let temp_snap_path = self.snap_path(SNAP_TEMP_BASENAME);
        {
            // first create temporary snapshot file
            snap.save_to_file(&temp_snap_path)
                .map_err(|err| anyhow::anyhow!("cannot save main snapshot: {err}"))?;
            // then overwrite main snapshot file
            std::fs::rename(&temp_snap_path, &main_snap_path)
                .map_err(|err| anyhow::anyhow!("cannot overwrite main snapshot: {err}"))?;
        }

        // create hard links for synced remotes
        for sync_path in synced_remotes {
            let sync_snap_path = self.snap_path(&Self::path_to_unique_id(sync_path));
            // first create a hard link to the main snapshot
            std::fs::hard_link(&main_snap_path, &temp_snap_path).map_err(|err| {
                anyhow::anyhow!(
                    "cannot create hard link to main snapshot for sync {sync_path}: {err}"
                )
            })?;
            // then overwrite the target path
            std::fs::rename(&temp_snap_path, sync_snap_path).map_err(|err| {
                anyhow::anyhow!("cannot overwrite sync snapshot {sync_path}: {err}")
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::config::tests::load_ut_cfg;
    use crate::proto::MetadataSnap;
    use std::sync::Arc;

    #[test]
    fn test_path_to_unique_id() {
        assert_eq!(
            SnapAccess::path_to_unique_id("/data/folder"),
            "65527a0fc2f2ee72892670a097313e5e"
        );
    }

    #[test]
    fn test_snap_access_save_load() -> anyhow::Result<()> {
        crate::generic::test::log_init();

        let temp_dir = tempfile::tempdir()?;

        // load config, patched to point to temp dir
        let mut cfg = load_ut_cfg().unwrap();
        cfg.local_metadata_snap_path_user = temp_dir.path().to_path_buf();
        let cfg = Arc::new(cfg);

        let source_path = "/my/source/path";
        let snap_access = SnapAccess::new(&cfg, source_path);

        // Create a dummy snapshot
        let snap = MetadataSnap {
            ts: None,
            path: source_path.to_string(),
            last_syncs: std::collections::BTreeMap::new(),
            root: None,
        };

        let remote_path = "/remote/path";

        // Save snapshot
        snap_access.save_snap(&snap, &[remote_path])?;

        // Verify main snapshot exists and matches
        let loaded_snap = snap_access.load_main_snap().expect("Should load main snap");
        assert_eq!(loaded_snap, snap);

        // Verify sync snapshot exists and matches
        let loaded_sync_snap = snap_access
            .load_sync_snap(remote_path)
            .expect("Should load sync snap");
        assert_eq!(loaded_sync_snap, snap);

        // Verify that a non-existent sync path returns None
        assert!(snap_access.load_sync_snap("/other/remote").is_none());

        Ok(())
    }
}
