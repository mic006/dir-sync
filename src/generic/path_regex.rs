//! Path regex manipulation

/// Builder for regex matching some name/paths
pub struct PathRegexBuilder {
    /// Function to generate one regex block from a path pattern
    pattern_builder: fn(&str) -> String,
    /// Build final regex matching any input pattern
    re: String,
    /// First / subsequent pattern insertion
    subsequent: bool,
}
impl PathRegexBuilder {
    /// Generate a name regex from path-like patterns
    ///
    /// "*" represents any number of characters, including leading ".".
    /// The resulting regex will match any of the input patterns.
    #[must_use]
    pub fn new_name() -> Self {
        Self {
            pattern_builder: gen_name_pattern,
            re: String::with_capacity(4096),
            subsequent: false,
        }
    }

    /// Generate a path regex from path-like patterns
    ///
    /// "*" represents any number of characters excluding "/".
    /// The resulting regex will match any of the input patterns.
    #[must_use]
    pub fn new_path() -> Self {
        Self {
            pattern_builder: gen_path_pattern,
            re: String::with_capacity(4096),
            subsequent: false,
        }
    }

    /// Add matching pattern in the regex
    pub fn add_pattern(&mut self, pattern: &str) {
        if self.subsequent {
            self.re.push('|');
        } else {
            self.re.push_str("^(?:");
            self.subsequent = true;
        }
        self.re.push_str(&(self.pattern_builder)(pattern));
    }

    /// Generate the regex matching any of the input patterns
    ///
    /// # Returns
    /// * Ok(Some(regex)): regex matching the added patterns
    /// * Ok(None): no pattern added
    ///
    /// # Errors
    /// * invalid final regex (unexpected, as regex characters in input patterns are escaped)
    pub fn finalize(mut self) -> anyhow::Result<Option<regex::Regex>> {
        if !self.subsequent {
            // no pattern added
            return Ok(None);
        }
        self.re.push_str(")$");
        Ok(Some(regex::Regex::new(&self.re)?))
    }
}

/// Generate a name regex pattern from a path-like pattern
///
/// "*" represents any number of characters, including leading "."
fn gen_name_pattern(pattern: &str) -> String {
    regex::escape(pattern).replace("\\*", ".*")
}

/// Generate a path regex pattern from a path-like pattern
///
/// "*" represents any number of characters excluding "/"
fn gen_path_pattern(pattern: &str) -> String {
    regex::escape(pattern).replace("\\*", "[^/]*")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_empty_regex() {
        let builder = PathRegexBuilder::new_name();
        let re = builder.finalize().unwrap();
        assert!(re.is_none());
    }

    #[test]
    fn test_build_name_regex() {
        let mut builder = PathRegexBuilder::new_name();
        for pattern in ["*.bak", "*.o", "*.old", "*~", ".git", "[12].gz"] {
            builder.add_pattern(pattern);
        }
        let re = builder.finalize().unwrap().unwrap();

        assert!(re.is_match("foo.bak"));
        assert!(!re.is_match("foo.bak.gz"));
        assert!(re.is_match("bar.cpp.o"));
        assert!(!re.is_match("bar.cpp.o2"));
        assert!(re.is_match("bar.old"));
        assert!(re.is_match(".foo.bar~"));
        assert!(re.is_match(".git"));
        assert!(!re.is_match("alpha.git"));

        assert!(re.is_match("[12].gz"));
        assert!(!re.is_match("1.gz"));
    }

    #[test]
    fn test_gen_path_regex() {
        let mut builder = PathRegexBuilder::new_path();
        for pattern in ["*/bar", "foo/$baz/*/*.gz"] {
            builder.add_pattern(pattern);
        }
        let re = builder.finalize().unwrap().unwrap();

        assert!(re.is_match("xxx/bar"));
        assert!(!re.is_match("bar"));
        assert!(!re.is_match("xxx/yyy/bar"));
        assert!(re.is_match("foo/$baz/toto/xxx.gz"));
        assert!(!re.is_match("foo/$baz/toto/xxx.gz.bak"));
        assert!(!re.is_match("foo/baz/toto/xxx.gz"));
    }
}
