//! Manipulate &'static str and String in a transparent way

/// Container for `&'static str` or `String`
#[derive(Clone, PartialEq, Debug)]
pub enum StrOrString {
    /// cheap reference to a constant str
    Str(&'static str),
    /// owned string
    String(String),
}
impl Default for StrOrString {
    fn default() -> Self {
        Self::Str("")
    }
}
impl StrOrString {
    /// Create owned string from `&str`
    #[must_use]
    pub fn from_str_to_owned(s: &str) -> Self {
        StrOrString::String(s.to_owned())
    }

    /// Get content as `&str`
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            StrOrString::Str(s) => s,
            StrOrString::String(s) => s,
        }
    }

    #[must_use]
    pub fn vec_to_str(value: &[StrOrString]) -> Vec<&str> {
        value.iter().map(StrOrString::as_str).collect()
    }

    #[must_use]
    pub fn vec_vec_to_str(value: &[Vec<StrOrString>]) -> Vec<Vec<&str>> {
        value.iter().map(|v| StrOrString::vec_to_str(v)).collect()
    }
}
impl From<&'static str> for StrOrString {
    fn from(value: &'static str) -> Self {
        StrOrString::Str(value)
    }
}
impl From<String> for StrOrString {
    fn from(value: String) -> Self {
        StrOrString::String(value)
    }
}

impl std::fmt::Display for StrOrString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn str_or_string() {
        let v: Vec<StrOrString> = vec!["alpha".into(), String::from("beta").into()];
        assert_eq!(v[0].to_string(), "alpha");
        assert_eq!(v[1].to_string(), "beta");
    }

    #[test]
    fn str_or_string_vec_to_str() {
        let v: Vec<StrOrString> = vec!["alpha".into(), String::from("beta").into()];
        assert_eq!(StrOrString::vec_to_str(&v), vec!["alpha", "beta"]);
    }

    #[test]
    fn str_or_string_vec_vec_to_str() {
        let v: Vec<Vec<StrOrString>> = vec![
            vec!["alpha".into(), String::from("beta").into()],
            vec![],
            vec!["gamma".into()],
        ];
        assert_eq!(
            StrOrString::vec_vec_to_str(&v),
            vec![vec!["alpha", "beta"], vec![], vec!["gamma"]]
        );
    }
}
