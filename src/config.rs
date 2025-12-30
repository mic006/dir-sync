//! Dir-sync user config, in YAML format

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::Context as _;
use serde::Deserialize;

/// Default configuration path
pub const DEFAULT_CFG_PATH: &str = "/etc/dir-sync.conf.yaml";

/// Configuration reference, shared between all objects (read-only access)
pub type ConfigRef = std::sync::Arc<Config>;

/// Dir-sync configuration
#[derive(Deserialize, Debug, PartialEq)]
pub struct Config {
    profiles: BTreeMap<String, Profile>,
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
}
impl FromStr for Config {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(serde_yaml::from_str(s)?)
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
pub struct Profile {
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
    consider: Vec<PathBuf>,
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[allow(clippy::missing_errors_doc)]
    pub fn load_ut_cfg() -> anyhow::Result<Config> {
        Config::from_file(Some(Path::new("src/test/ut_config.yaml")))
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn load_cfg() {
        let cfg = load_ut_cfg().unwrap();
        let expected_cfg = Config {
            profiles: BTreeMap::from([
                (
                    String::from("base"),
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
                        consider: vec![],
                    },
                ),
                (
                    String::from("data"),
                    Profile {
                        include: ZeroOrOneOrList::One(String::from("base")),
                        ignore_name: vec![],
                        ignore_path: vec![PathBuf::from("*/bar")],
                        consider: vec![PathBuf::from("folder/foo~/toto.bak")],
                    },
                ),
            ]),
        };
        assert_eq!(cfg, expected_cfg);
    }
}
