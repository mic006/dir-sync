//! Iterator extension

pub trait CommonValueExt: Iterator {
    /// Check if iterator values are all equal, and return that value
    ///
    /// Note: iteration stops on first mismatch (short-circuiting)
    ///
    /// # Returns
    /// - Some(v) when all values are equal, v being the first value
    /// - None when not all values are equal, or iterator is empty
    fn common_value(self) -> Option<Self::Item>;
}

impl<I: Iterator> CommonValueExt for I
where
    Self::Item: PartialEq,
{
    fn common_value(self) -> Option<Self::Item> {
        self.common_value_by(Self::Item::eq)
    }
}

pub trait CommonValueByExt: Iterator {
    /// Check if iterator values are all equal using custom equality function, and return that value
    ///
    /// Note: iteration stops on first mismatch (short-circuiting)
    ///
    /// # Returns
    /// - Some(v) when all values are equal, v being the first value
    /// - None when not all values are equal, or iterator is empty
    fn common_value_by<F>(self, eq: F) -> Option<Self::Item>
    where
        F: FnMut(&Self::Item, &Self::Item) -> bool;
}

impl<I: Iterator> CommonValueByExt for I {
    fn common_value_by<F>(mut self, mut eq: F) -> Option<Self::Item>
    where
        F: FnMut(&Self::Item, &Self::Item) -> bool,
    {
        self.try_fold(None, |first, next| {
            if let Some(first) = first {
                if eq(&first, &next) {
                    // still matching
                    Ok(Some(first))
                } else {
                    // not matching anymore
                    Err(())
                }
            } else {
                // first value
                Ok(Some(next))
            }
        })
        .ok()
        .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_value() {
        assert_eq!(vec![0; 0].into_iter().common_value(), None);
        assert_eq!(vec![1].into_iter().common_value(), Some(1));
        assert_eq!(vec![1, 1].into_iter().common_value(), Some(1));
        assert_eq!(vec![1, 2].into_iter().common_value(), None);
        assert_eq!(vec![1, 1, 1].into_iter().common_value(), Some(1));
        assert_eq!(vec![1, 2, 1].into_iter().common_value(), None);
    }
}
