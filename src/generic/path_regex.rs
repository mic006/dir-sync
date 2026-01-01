//! Path regex manipulation

/// Generate a name regex from path-like patterns
///
/// "*" represents any number of characters, including leading ".".
/// The resulting regex will match any of the input patterns.
///
/// # Errors
/// * invalid final regex (unexpected, as regex characters in input patterns are escaped)
pub fn gen_name_regex<I>(patterns: I) -> anyhow::Result<regex::Regex>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut re = String::with_capacity(1024);
    re.push_str("^(?:");
    for pattern in patterns {
        re.push_str(&gen_name_pattern(pattern.as_ref()));
        re.push('|');
    }
    re.push_str(")$");
    Ok(regex::Regex::new(&re)?)
}

/// Generate a path regex from path-like patterns
///
/// "*" represents any number of characters excluding "/".
/// The resulting regex will match any of the input patterns.
///
/// # Errors
/// * invalid final regex (unexpected, as regex characters in input patterns are escaped)
pub fn gen_path_regex<I>(patterns: I) -> anyhow::Result<regex::Regex>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut re = String::with_capacity(1024);
    re.push_str("^(?:");
    for pattern in patterns {
        re.push_str(&gen_path_pattern(pattern.as_ref()));
        re.push('|');
    }
    re.push_str(")$");
    Ok(regex::Regex::new(&re)?)
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
    fn test_gen_name_regex() {
        let patterns = vec!["*.bak", "*.o", "*.old", "*~", ".git", "[12].gz"];
        let re = gen_name_regex(&patterns).unwrap();

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
        let patterns = vec!["*/bar", "foo/$baz/*/*.gz"];
        let re = gen_path_regex(&patterns).unwrap();

        assert!(re.is_match("xxx/bar"));
        assert!(!re.is_match("bar"));
        assert!(!re.is_match("xxx/yyy/bar"));
        assert!(re.is_match("foo/$baz/toto/xxx.gz"));
        assert!(!re.is_match("foo/$baz/toto/xxx.gz.bak"));
        assert!(!re.is_match("foo/baz/toto/xxx.gz"));
    }
}
