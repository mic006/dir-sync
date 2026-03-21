//! Scroll bar helper

/*
/// Scroll bar is on the right side of the screen
/// Use octant unicode characters to display the scroll bar on the right half side
/// Array is indexed by bitmap value of 4 bits, bit #0 at top right and bit #3 at bottom right
const SB_OCTANT: [char; 16] = [
    '\u{20}',    // 0000
    '\u{1CEAB}', // 0001
    '\u{1CD03}', // 0010
    '\u{259D}',  // 0011
    '\u{1CD18}', // 0100
    '\u{1CD1A}', // 0101
    '\u{1FBE7}', // 0110
    '\u{1CD21}', // 0111
    '\u{1CEA0}', // 1000
    '\u{1CD72}', // 1001
    '\u{1CD78}', // 1010
    '\u{1CD7A}', // 1011
    '\u{2597}',  // 1100
    '\u{1CD91}', // 1101
    '\u{1CD96}', // 1110
    '\u{2590}',  // 1111
];
*/

/// Scroll bar drawing with unicode characters
///
/// Indexed by (length-1) first, then by start index
const SB_OCTANT: [[char; 4]; 4] = [
    [
        '\u{1CEA8}', // 0001
        '\u{1CD00}', // 0010
        '\u{1CD09}', // 0100
        '\u{1CEA3}', // 1000
    ],
    [
        '\u{2598}',  // 0011
        '\u{1FBE6}', // 0110
        '\u{2596}',  // 1100
        ' ',         // N/A
    ],
    [
        '\u{1CD0D}', // 0111
        '\u{1CD48}', // 1110
        ' ',         // N/A
        ' ',         // N/A
    ],
    [
        '\u{258C}', // 1111
        ' ',        // N/A
        ' ',        // N/A
        ' ',        // N/A
    ],
];

/// Scroll bar result
#[derive(PartialEq, Debug, Default, Clone)]
pub struct ScrollBar {
    /// Start index
    pub start: usize,
    /// Length of scroll bar
    pub length: usize,
}
impl ScrollBar {
    pub fn new(
        view_start: usize,     // index of first visible item
        view_length: usize,    // number of visible items
        content_length: usize, // number of items in the collection
    ) -> Self {
        // using octants characters, the scroll bar uses 4 sub-blocks per line
        Self::calc(view_start, view_length, content_length, 4 * view_length)
    }

    pub fn iter(&self) -> Self {
        self.clone()
    }

    fn calc(
        view_start: usize,     // index of first visible item
        view_length: usize,    // number of visible items
        content_length: usize, // number of items in the collection
        sb_max_length: usize,  // maximum length of scroll bar
    ) -> Self {
        if view_length >= content_length {
            // no scroll bar
            return Self::default();
        }

        let mut length = rounded_div(view_length * sb_max_length, content_length);
        if view_length == content_length - 1 {
            // two positions, either [0..view_length) or [1..content_length)
            // => need at least one free item in scroll bar to show the position
            length = length.min(sb_max_length - 1);
        } else {
            // => need at least two free items in scroll bar to differentiate begin, middle and end
            length = length.min(sb_max_length - 2);
        }
        // scroll bar need to be visible
        length = length.max(1);

        let mut start = rounded_div(view_start * sb_max_length, content_length);
        if view_start > 0 {
            // show that scroll bar is not at the beginning
            start = start.max(1);
        }
        if view_start + view_length >= content_length {
            // show that scroll bar is at the end
            start = sb_max_length - length;
        } else {
            // show that scroll bar is not at the end
            start = start.min(sb_max_length - length - 1);
        }

        Self { start, length }
    }
}

impl Iterator for ScrollBar {
    type Item = (usize, char);

    fn next(&mut self) -> Option<Self::Item> {
        if self.length == 0 {
            return None;
        }

        let char_index = self.start / 4;
        let start_bit = self.start % 4;
        let l = if start_bit + self.length > 4 {
            4 - start_bit
        } else {
            self.length
        };

        // advance
        self.start = 4 * (char_index + 1);
        self.length -= l;

        Some((char_index, SB_OCTANT[l - 1][start_bit]))
    }
}

/// Divide a/b, get nearest integer
fn rounded_div(a: usize, b: usize) -> usize {
    (a + b / 2) / b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rounded_div() {
        assert_eq!(rounded_div(10, 3), 3);
        assert_eq!(rounded_div(11, 3), 4);
        assert_eq!(rounded_div(1, 2), 1);
        assert_eq!(rounded_div(0, 5), 0);
    }

    #[test]
    fn test_calc_no_scroll() {
        assert_eq!(ScrollBar::calc(0, 10, 10, 100), ScrollBar::default());
        assert_eq!(ScrollBar::calc(0, 20, 10, 100), ScrollBar::default());
    }

    #[test]
    fn test_calc_basic() {
        // content=100, view=10, max=10
        // length = 10*10/100 = 1.
        assert_eq!(
            ScrollBar::calc(0, 10, 100, 10),
            ScrollBar {
                start: 0,
                length: 1
            }
        );
        // middle
        assert_eq!(
            ScrollBar::calc(45, 10, 100, 10),
            ScrollBar {
                start: 5,
                length: 1
            }
        );
        // end
        assert_eq!(
            ScrollBar::calc(90, 10, 100, 10),
            ScrollBar {
                start: 9,
                length: 1
            }
        );
    }

    #[test]
    fn test_calc_not_start() {
        // content=1000, view=10, max=10
        // raw start = 1*10/1000 = 0.
        // adjustment: not start -> max(1) -> 1.
        assert_eq!(
            ScrollBar::calc(1, 10, 1000, 10),
            ScrollBar {
                start: 1,
                length: 1
            }
        );
    }

    #[test]
    fn test_calc_not_end() {
        // content=1000, view=10, max=10
        // view_start=989. (989+10 = 999 < 1000). Not end.
        // raw start = 989*10/1000 = 9.89 -> 10.
        // length=1.
        // max valid start if not end = 10 - 1 - 1 = 8.
        assert_eq!(
            ScrollBar::calc(989, 10, 1000, 10),
            ScrollBar {
                start: 8,
                length: 1
            }
        );
    }

    #[test]
    fn test_calc_large_thumb() {
        // content=100, view=50, max=10.
        // length = 50*10/100 = 5.
        // max length allowed: 10 - 2 = 8.
        assert_eq!(
            ScrollBar::calc(0, 50, 100, 10),
            ScrollBar {
                start: 0,
                length: 5
            }
        );
    }

    #[test]
    fn test_calc_almost_full_view() {
        // content=100, view=99, max=10
        // length = rounded_div(99*10, 100) = 10.
        // adjustment for almost full: min(10-1) = 9.
        // view_start = 0:
        //   raw_start = 0.
        //   not_end adjustment: start.min(10-9-1=0) -> 0.
        assert_eq!(
            ScrollBar::calc(0, 99, 100, 10),
            ScrollBar {
                start: 0,
                length: 9
            }
        );
        // view_start = 1:
        //   raw_start = rounded_div(1*10, 100) = 0.
        //   not_start adjustment: start.max(1) -> 1.
        assert_eq!(
            ScrollBar::calc(1, 99, 100, 10),
            ScrollBar {
                start: 1,
                length: 9
            }
        );
    }

    #[test]
    fn test_calc_almost_full_view_2() {
        // content=100, view=98, max=10
        // length = rounded_div(98*10, 100) = 10.
        // adjustment: min(10-2) = 8.
        // view_start = 0:
        //   raw_start = 0.
        //   not_end adjustment: start.min(10-8-1=1) -> 0.
        assert_eq!(
            ScrollBar::calc(0, 98, 100, 10),
            ScrollBar {
                start: 0,
                length: 8
            }
        );
        // view_start = 1:
        //   raw_start = 0.
        //   start.max(1) -> 1.
        //   not_end adjustment: start.min(1) -> 1.
        assert_eq!(
            ScrollBar::calc(1, 98, 100, 10),
            ScrollBar {
                start: 1,
                length: 8
            }
        );
        // view_start = 2:
        //   raw_start = 0.
        //   start.max(1) -> 1.
        assert_eq!(
            ScrollBar::calc(2, 98, 100, 10),
            ScrollBar {
                start: 2,
                length: 8
            }
        );
    }

    #[test]
    fn test_iter() {
        // Start aligned, length full block
        let mut sb = ScrollBar {
            start: 0,
            length: 4,
        };
        assert_eq!(sb.next(), Some((0, '\u{258C}')));
        assert_eq!(sb.next(), None);

        // Start aligned, length > block
        let mut sb = ScrollBar {
            start: 0,
            length: 6,
        };
        // 0..4 (full)
        assert_eq!(sb.next(), Some((0, '\u{258C}')));
        // 4..6 (length 2, start bit 0 of next char)
        // SB_OCTANT[1][0] -> '\u{2598}'
        assert_eq!(sb.next(), Some((1, '\u{2598}')));
        assert_eq!(sb.next(), None);

        // Start unaligned
        let mut sb = ScrollBar {
            start: 2,
            length: 7,
        };
        // 2..4 (length 2, start bit 2)
        // SB_OCTANT[1][2] -> '\u{2596}'
        assert_eq!(sb.next(), Some((0, '\u{2596}')));
        // 4..8 (length 4, start bit 0) -> full
        assert_eq!(sb.next(), Some((1, '\u{258C}')));
        // 8..9 (length 1, start bit 0)
        // SB_OCTANT[0][0] -> '\u{1CEA8}'
        assert_eq!(sb.next(), Some((2, '\u{1CEA8}')));
        assert_eq!(sb.next(), None);
    }
}
