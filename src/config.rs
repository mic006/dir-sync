//! Dir-sync user config, in YAML format

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context as _;
use serde::Deserialize;

use crate::generic::config::field_mem_size::MemSize;
use crate::generic::path_regex::{self, PathRegexBuilder};
use crate::proto;
use crate::tui::theme::AppTheme;

/// Default configuration path
pub const DEFAULT_CFG_PATH: &str = "/etc/dir-sync.conf.yaml";

/// Configuration reference, shared between all objects (read-only access)
pub type ConfigRef = Arc<ConfigCtx>;

/// TUI Configuration reference, shared between all objects (read-only access)
pub type TuiConfigRef = Arc<TuiConfigCtx>;

/// Dir-sync configuration, deserialization of YAML config file
#[derive(Deserialize, Debug, PartialEq)]
pub struct Config {
    /// Path to store metadata snapshot of local paths, to speed-up further analysis
    /// Data will be stored in a sub-folder named with the dir-sync UID
    /// Sub-folders are NOT created by dir-sync, they shall be created manually
    local_metadata_snap_path: PathBuf,

    /// Performance settings
    #[serde(default)]
    performance: PerformanceCfg,

    /// Profile configuration, allowing user to select one configuration when launching a dir-sync instance
    profiles: BTreeMap<String, Profile>,

    /// Name of default profile, when profile is not specified on the command line
    default_profile: Option<String>,

    /// Theme file for Terminal UI rendering
    pub theme: Option<PathBuf>,

    /// Terminal UI related settings
    #[serde(default)]
    pub tui: TuiConfig,
}

impl Config {
    /// Read configuration from file
    ///
    /// # Errors
    /// * invalid path
    /// * invalid format
    pub fn from_file(path: Option<&Path>) -> anyhow::Result<Self> {
        let path = path.unwrap_or(Path::new(DEFAULT_CFG_PATH));
        let config_str = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file at '{}'", path.display()))?;
        Self::from_str(&config_str)
    }

    /// Get configuration context from configuration file
    ///
    /// # Errors
    /// * invalid profile (profile not found, or include profile not found)
    pub fn extract(self, profile: Option<&str>) -> anyhow::Result<ConfigRef> {
        Ok(self.extract_internal(profile, false)?.0)
    }

    /// Get configuration context + TUI config context from configuration file
    ///
    /// # Errors
    /// * invalid profile (profile not found, or include profile not found)
    pub fn extract_tui(self, profile: Option<&str>) -> anyhow::Result<(ConfigRef, TuiConfigRef)> {
        let (config, tui_config) = self.extract_internal(profile, true)?;
        Ok((config, tui_config.unwrap()))
    }

    fn extract_internal(
        mut self,
        profile: Option<&str>,
        tui_mode: bool,
    ) -> anyhow::Result<(ConfigRef, Option<TuiConfigRef>)> {
        let local_metadata_snap_path_user = self
            .local_metadata_snap_path
            .join(std::env::var("USER").unwrap_or_else(|_| String::from("nobody")));

        let mut filter_ignore_name = vec![];
        let mut filter_ignore_path = vec![];
        let mut filter_white_list = vec![];

        if let Some(profile) = profile {
            let mut profiles = vec![profile.to_string()];

            while let Some(prof_name) = profiles.pop() {
                let Some(mut prof_data) = self.profiles.remove(&prof_name) else {
                    anyhow::bail!("config error: invalid reference to profile '{prof_name}'");
                };
                profiles.extend(prof_data.include.iter().map(ToOwned::to_owned));
                filter_ignore_name.append(&mut prof_data.ignore_name);
                filter_ignore_path.append(&mut prof_data.ignore_path);
                filter_white_list.extend(
                    prof_data
                        .white_list
                        .iter()
                        .map(|pattern| path_regex::add_rel_path_prefix(pattern)),
                );
            }
        }

        let mut config_ctx = ConfigCtx {
            local_metadata_snap_path_user,
            perf_data_buffer_size: self.performance.data_buffer_size.as_nb_bytes(),
            perf_fs_queue_size: self.performance.fs_queue_size,
            filter_ignore_name,
            filter_ignore_path,
            filter_white_list,
            file_matcher: None,
        };
        config_ctx.add_file_matcher()?;

        let tui_config = if tui_mode {
            let theme = AppTheme::load(self.theme.as_deref())?;
            Some(Arc::new(TuiConfigCtx {
                tui: self.tui,
                theme,
            }))
        } else {
            None
        };

        Ok((Arc::new(config_ctx), tui_config))
    }
}
impl FromStr for Config {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(serde_yaml::from_str::<Self>(s)?)
    }
}

/// Per profile configuration
#[derive(Deserialize, Debug, PartialEq)]
#[serde(default)]
pub struct PerformanceCfg {
    /// Size of buffer to read / write file content
    pub data_buffer_size: MemSize,
    /// Size of queue for filesystem operations (number of read / write / delete operations)
    pub fs_queue_size: usize,
}
impl Default for PerformanceCfg {
    fn default() -> Self {
        Self {
            data_buffer_size: MemSize::new(64 * 1024),
            fs_queue_size: 8,
        }
    }
}

/// Terminal UI related settings
#[derive(Deserialize, Debug, PartialEq)]
pub struct TuiConfig {
    /// Maximum file size to read and display content
    /// Note: must be <= `performance.data_buffer_size`
    pub content_max_size: MemSize,
}
impl Default for TuiConfig {
    fn default() -> Self {
        Self {
            content_max_size: MemSize::new(32 * 1024),
        }
    }
}

/// Allow user to specify one element or a list
/// <https://www.reddit.com/r/rust/comments/l3exvd/comment/gkh6ibv/?utm_source=share&utm_medium=web3x&utm_name=web3xcss&utm_term=1&utm_content=share_button>
#[derive(Deserialize, Default, Debug, PartialEq)]
#[serde(untagged)]
pub enum ZeroOrOneOrList<T> {
    /// No element
    #[default]
    Zero,
    /// Single element
    One(T),
    /// Multiple elements
    List(Vec<T>),
}
impl<T> ::std::ops::Deref for ZeroOrOneOrList<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        match *self {
            ZeroOrOneOrList::Zero => &[],
            ZeroOrOneOrList::One(ref x) => std::slice::from_ref(x),
            ZeroOrOneOrList::List(ref x) => x.as_slice(),
        }
    }
}

/// Per profile configuration
#[derive(Deserialize, Debug, PartialEq)]
struct Profile {
    /// Include other profile(s)
    #[serde(default)]
    include: ZeroOrOneOrList<String>,
    /// File/directory names to be ignored, at any level
    /// "*" represents any number of characters, including leading "."
    #[serde(default)]
    ignore_name: Vec<String>,
    /// Relative paths to be ignored
    /// "*" represents any number of characters excluding "/"
    #[serde(default)]
    ignore_path: Vec<String>,
    /// Relative paths to be considered, even if a parent folder is ignored
    #[serde(default)]
    white_list: Vec<String>,
}

pub struct ConfigCtx {
    /// Path for current user = `local_metadata_snap_path/$USER`
    pub local_metadata_snap_path_user: PathBuf,

    // Performance settings
    pub perf_data_buffer_size: usize,
    pub perf_fs_queue_size: usize,

    /// File/directory names to be ignored, at any level
    /// "*" represents any number of characters, including leading "."
    pub filter_ignore_name: Vec<String>,
    /// Relative paths to be ignored
    /// "*" represents any number of characters excluding "/"
    pub filter_ignore_path: Vec<String>,
    /// Relative paths to be considered, even if a parent folder is ignored
    pub filter_white_list: Vec<String>,

    /// Determine name/paths to be ignored
    pub file_matcher: Option<FileMatcher>,
}

impl ConfigCtx {
    /// Get configuration context from configuration provided by master
    ///
    /// # Errors
    /// * internal error
    pub fn from_master_config(
        local_config: Config,
        master_config: proto::remote::Config,
    ) -> anyhow::Result<Self> {
        let local_metadata_snap_path_user = local_config
            .local_metadata_snap_path
            .join(std::env::var("USER").unwrap_or_else(|_| String::from("nobody")));

        let mut instance = Self {
            local_metadata_snap_path_user,
            perf_data_buffer_size: master_config.perf_data_buffer_size.try_into()?,
            perf_fs_queue_size: master_config.perf_fs_queue_size as usize,
            filter_ignore_name: master_config.filter_ignore_name,
            filter_ignore_path: master_config.filter_ignore_path,
            filter_white_list: master_config.filter_white_list,
            file_matcher: None,
        };
        instance.add_file_matcher()?;
        Ok(instance)
    }

    fn add_file_matcher(&mut self) -> anyhow::Result<()> {
        if !(self.filter_ignore_name.is_empty() && self.filter_ignore_path.is_empty()) {
            let mut ignore_name_regex_builder = PathRegexBuilder::new_name();
            for p in &self.filter_ignore_name {
                ignore_name_regex_builder.add_pattern(p);
            }
            let mut ignore_path_regex_builder = PathRegexBuilder::new_path();
            for p in &self.filter_ignore_path {
                ignore_path_regex_builder.add_pattern(p);
            }
            self.file_matcher = Some(FileMatcher {
                ignore_name_regex: ignore_name_regex_builder.finalize()?,
                ignore_path_regex: ignore_path_regex_builder.finalize()?,
                white_list: self.filter_white_list.clone(),
            });
        }
        Ok(())
    }
}

/// Terminal UI configuration context
pub struct TuiConfigCtx {
    pub tui: TuiConfig,
    pub theme: AppTheme,
}

/// Determine name/paths to be ignored
#[derive(Clone)]
pub struct FileMatcher {
    /// File/directory names to be ignored, at any level
    ignore_name_regex: Option<regex::Regex>,
    /// Relative paths to be ignored
    ignore_path_regex: Option<regex::Regex>,
    /// Relative paths to be considered, even if a parent folder is ignored
    white_list: Vec<String>,
}
impl FileMatcher {
    #[must_use]
    pub fn is_ignored(&self, rel_path: &str) -> bool {
        // white list: any path needed to access a considered entry
        if self
            .white_list
            .iter()
            .any(|consider| consider.starts_with(rel_path))
        {
            return false;
        }
        // ignore based on name or path
        let name = Path::new(rel_path).file_name().unwrap().to_str().unwrap();
        self.ignore_name_regex
            .as_ref()
            .is_some_and(|r| r.is_match(name))
            || self
                .ignore_path_regex
                .as_ref()
                .is_some_and(|r| r.is_match(rel_path))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[allow(clippy::missing_errors_doc)]
    pub fn load_ut_cfg() -> anyhow::Result<Config> {
        Config::from_file(Some(Path::new("src/test/ut_config.yaml")))
    }
    #[allow(clippy::missing_errors_doc)]
    pub fn load_ut_cfg_ctx() -> anyhow::Result<ConfigRef> {
        let config = Config::from_file(Some(Path::new("src/test/ut_config.yaml")))?;
        config.extract(Some("data"))
    }

    #[test]
    fn test_load_cfg() {
        let cfg = load_ut_cfg().unwrap();
        let expected_cfg = Config {
            local_metadata_snap_path: PathBuf::from("/invalid/path"),
            performance: PerformanceCfg {
                data_buffer_size: MemSize::new(64 * 1024),
                fs_queue_size: 16,
            },
            profiles: BTreeMap::from([
                (
                    String::from("default"),
                    Profile {
                        include: ZeroOrOneOrList::Zero,
                        ignore_name: vec![
                            String::from("*.bak"),
                            String::from("*.o"),
                            String::from("*.old"),
                            String::from("*~"),
                            String::from(".git"),
                        ],
                        ignore_path: vec![],
                        white_list: vec![],
                    },
                ),
                (
                    String::from("data"),
                    Profile {
                        include: ZeroOrOneOrList::One(String::from("default")),
                        ignore_name: vec![String::from("cache")],
                        ignore_path: vec![String::from("*/bar")],
                        white_list: vec![String::from("folder/foo~/toto.bak")],
                    },
                ),
            ]),
            default_profile: None,
            theme: Some(PathBuf::from("/path/to/theme/file")),
            tui: TuiConfig {
                content_max_size: MemSize::new(16 * 1024),
            },
        };
        assert_eq!(cfg, expected_cfg);
    }

    #[test]
    fn test_config_ctx_from_cfg_file() {
        let config = Config::from_file(Some(Path::new("src/test/ut_config.yaml"))).unwrap();
        let config = config.extract(Some("data")).unwrap();
        assert_eq!(
            config.local_metadata_snap_path_user,
            PathBuf::from("/invalid/path").join(std::env::var("USER").unwrap())
        );
        assert_eq!(config.perf_data_buffer_size, 64 * 1024);
        assert_eq!(config.perf_fs_queue_size, 16);
        assert_eq!(
            config.filter_ignore_name,
            vec![
                String::from("cache"),
                String::from("*.bak"),
                String::from("*.o"),
                String::from("*.old"),
                String::from("*~"),
                String::from(".git"),
            ]
        );
        assert_eq!(config.filter_ignore_path, vec![String::from("*/bar")]);
        assert_eq!(
            config.filter_white_list,
            vec![String::from("./folder/foo~/toto.bak")]
        );
        assert!(config.file_matcher.is_some());
    }

    #[test]
    fn test_config_ctx_from_cfg_file_profile_none() {
        let config = load_ut_cfg().unwrap();
        let config = config.extract(None).unwrap();
        assert!(config.file_matcher.is_none());
    }

    #[test]
    fn test_config_ctx_from_cfg_file_profile_invalid() {
        let config = load_ut_cfg().unwrap();
        let config = config.extract(Some("unknown"));
        assert!(config.is_err());
    }

    #[test]
    fn test_cfg_file_matcher() {
        let cfg = load_ut_cfg_ctx().unwrap();
        assert!(cfg.file_matcher.is_some());
        let file_matcher = cfg.file_matcher.as_ref().unwrap();
        assert!(file_matcher.is_ignored("./toto.bak"));
        assert!(!file_matcher.is_ignored("./bar"));
        assert!(file_matcher.is_ignored("./foo/bar"));
        assert!(!file_matcher.is_ignored("./foo/sub/bar"));
        assert!(file_matcher.is_ignored("./folder/baz~"));
        assert!(!file_matcher.is_ignored("./folder/foo~"));
        assert!(!file_matcher.is_ignored("./folder/foo~/toto.bak"));
        assert!(file_matcher.is_ignored("./folder/foo~/titi.bak"));
    }
}
