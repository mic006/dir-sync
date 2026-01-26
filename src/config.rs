//! Dir-sync user config, in YAML format

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::Context as _;
use serde::Deserialize;

use crate::generic::fs::PathExt as _;
use crate::generic::path_regex::PathRegexBuilder;

/// Default configuration path
pub const DEFAULT_CFG_PATH: &str = "/data/develop/github/dir-sync/etc/dir-sync.conf.yaml"; // TODO: "/etc/dir-sync.conf.yaml";

/// Configuration reference, shared between all objects (read-only access)
pub type ConfigRef = std::sync::Arc<Config>;

/// Dir-sync configuration
#[derive(Deserialize, Debug, PartialEq)]
pub struct Config {
    /// Path to store metadata snapshot of local paths, to speed-up further analysis
    /// Data will be stored in a sub-folder named with the dir-sync UID
    /// Sub-folders are NOT created by dir-sync, they shall be created manually
    local_metadata_snap_path: PathBuf,
    /// Path for current user = `local_metadata_snap_path/$USER`
    #[serde(skip)]
    pub local_metadata_snap_path_user: PathBuf,

    /// Profile configuration, allowing user to select one configuration when launching a dir-sync instance
    profiles: BTreeMap<String, Profile>,

    /// Name of default profile, when profile is not specified on the command line
    default_profile: Option<String>,
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

    /// Finalize configuration
    fn finalize(&mut self) {
        self.local_metadata_snap_path_user = self
            .local_metadata_snap_path
            .join(std::env::var("USER").unwrap_or_else(|_| String::from("nobody")));
    }

    /// Get file matcher for the wanted profile
    ///
    /// # Errors
    /// * invalid profile (profile not found, or include profile not found)
    pub fn get_file_matcher(&self, profile: Option<&str>) -> anyhow::Result<Option<FileMatcher>> {
        let Some(profile) = profile.or(self.default_profile.as_deref()) else {
            return Ok(None);
        };

        let mut ignore_name_regex_builder = PathRegexBuilder::new_name();
        let mut ignore_path_regex_builder = PathRegexBuilder::new_path();
        let mut white_list = vec![];

        let mut profiles = vec![profile];

        while let Some(prof_name) = profiles.pop() {
            let Some(prof_data) = self.profiles.get(prof_name) else {
                anyhow::bail!("config error: invalid reference to profile '{prof_name}'");
            };
            for p in &*prof_data.include {
                profiles.push(p);
            }
            for p in &prof_data.ignore_name {
                ignore_name_regex_builder.add_pattern(p);
            }
            for p in &prof_data.ignore_path {
                ignore_path_regex_builder.add_pattern(p.checked_as_str()?);
            }
            white_list.extend_from_slice(&prof_data.white_list);
        }

        Ok(Some(FileMatcher {
            ignore_name_regex: ignore_name_regex_builder.finalize()?,
            ignore_path_regex: ignore_path_regex_builder.finalize()?,
            white_list,
        }))
    }
}
impl FromStr for Config {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut instance = serde_yaml::from_str::<Self>(s)?;
        instance.finalize();
        Ok(instance)
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
    ignore_path: Vec<PathBuf>,
    /// Relative paths to be considered, even if a parent folder is ignored
    #[serde(default)]
    white_list: Vec<PathBuf>,
}

/// Determine name/paths to be ignored
#[derive(Clone)]
pub struct FileMatcher {
    /// File/directory names to be ignored, at any level
    ignore_name_regex: Option<regex::Regex>,
    /// Relative paths to be ignored
    ignore_path_regex: Option<regex::Regex>,
    /// Relative paths to be considered, even if a parent folder is ignored
    white_list: Vec<PathBuf>,
}
impl FileMatcher {
    #[must_use]
    pub fn is_ignored(&self, rel_path: &Path) -> bool {
        // white list: any path needed to access a considered entry
        if self
            .white_list
            .iter()
            .any(|consider| consider.starts_with(rel_path))
        {
            return false;
        }
        // ignore based on name or path
        let name = rel_path.file_name().unwrap().to_str().unwrap();
        self.ignore_name_regex
            .as_ref()
            .is_some_and(|r| r.is_match(name))
            || self
                .ignore_path_regex
                .as_ref()
                .is_some_and(|r| r.is_match(rel_path.checked_as_str().unwrap()))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[allow(clippy::missing_errors_doc)]
    pub fn load_ut_cfg() -> anyhow::Result<Config> {
        Config::from_file(Some(Path::new("src/test/ut_config.yaml")))
    }

    #[test]
    fn load_cfg() {
        let cfg = load_ut_cfg().unwrap();
        let expected_cfg = Config {
            local_metadata_snap_path: PathBuf::from("/invalid/path"),
            local_metadata_snap_path_user: PathBuf::from("/invalid/path")
                .join(std::env::var("USER").unwrap()),
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
                        ignore_name: vec![],
                        ignore_path: vec![PathBuf::from("*/bar")],
                        white_list: vec![PathBuf::from("folder/foo~/toto.bak")],
                    },
                ),
            ]),
            default_profile: None,
        };
        assert_eq!(cfg, expected_cfg);
    }

    #[test]
    fn test_cfg_get_file_matcher() {
        let cfg = load_ut_cfg().unwrap();
        assert!(cfg.get_file_matcher(Some("unknown")).is_err());
        assert!(cfg.get_file_matcher(None).unwrap().is_none());

        let file_matcher = cfg.get_file_matcher(Some("data")).unwrap().unwrap();

        assert!(file_matcher.is_ignored(Path::new("toto.bak")));
        assert!(!file_matcher.is_ignored(Path::new("bar")));
        assert!(file_matcher.is_ignored(Path::new("foo/bar")));
        assert!(!file_matcher.is_ignored(Path::new("foo/sub/bar")));
        assert!(file_matcher.is_ignored(Path::new("folder/baz~")));
        assert!(!file_matcher.is_ignored(Path::new("folder/foo~")));
        assert!(!file_matcher.is_ignored(Path::new("folder/foo~/toto.bak")));
        assert!(file_matcher.is_ignored(Path::new("folder/foo~/titi.bak")));
    }
}
