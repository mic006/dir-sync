//! Print tree like `ls -l`

use std::fmt::Write as _;

/// Context to display a tree
#[derive(Default)]
struct FormatTree {
    /// Bitmap of active subtrees
    active_bitmap: u64,
    /// Next level
    next_level: u8,
}

impl FormatTree {
    pub fn entering_sub(&mut self) {
        debug_assert!(self.next_level < 64);
        self.active_bitmap |= 1 << self.next_level;
        self.next_level += 1;
    }

    pub fn leaving_sub(&mut self) {
        debug_assert!(self.next_level > 0);
        self.next_level -= 1;
    }

    pub fn new_entry(&mut self, last: bool) -> String {
        if self.next_level == 0 {
            return String::new();
        }
        let level = self.next_level - 1;

        if last {
            self.active_bitmap &= !(1 << level);
        }

        let mut res = String::with_capacity(level as usize * 7);

        for i in 0..level {
            if (self.active_bitmap >> i) & 1 == 0 {
                write!(&mut res, "   ").unwrap();
            } else {
                write!(&mut res, "│  ").unwrap();
            }
        }
        if last {
            write!(&mut res, "└─ ").unwrap();
        } else {
            write!(&mut res, "├─ ").unwrap();
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_tree() {
        let mut tree = FormatTree::default();

        assert_eq!(tree.new_entry(false), "");
        tree.entering_sub();
        assert_eq!(tree.new_entry(false), "├─ ");
        tree.entering_sub();
        assert_eq!(tree.new_entry(false), "│  ├─ ");
        assert_eq!(tree.new_entry(true), "│  └─ ");
        tree.leaving_sub();
        assert_eq!(tree.new_entry(false), "├─ ");
        assert_eq!(tree.new_entry(true), "└─ ");
        tree.entering_sub();
        assert_eq!(tree.new_entry(false), "   ├─ ");
        assert_eq!(tree.new_entry(true), "   └─ ");
        tree.leaving_sub();
        tree.leaving_sub();
    }
}
