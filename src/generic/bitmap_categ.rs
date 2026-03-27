//! Use bitmap to split a set of indexed elements in two categories

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Categ {
    ZERO,
    ONE,
}

/// Bitmap to split a set of indexed elements in two categories
#[derive(Default, PartialEq, Debug, Clone, Copy)]
pub struct BitmapCateg(u32);

impl BitmapCateg {
    /// Set categ value for one index
    pub fn set(&mut self, index: usize, categ: Categ) {
        match categ {
            Categ::ZERO => self.0 &= !(1 << index),
            Categ::ONE => self.0 |= 1 << index,
        }
    }

    /// Get categ value for one index
    #[must_use]
    pub fn get(&self, index: usize) -> Categ {
        if self.0 & (1 << index) != 0 {
            Categ::ONE
        } else {
            Categ::ZERO
        }
    }

    /// Get first index for categ value
    #[must_use]
    pub fn get_first(&self, categ: Categ) -> usize {
        match categ {
            Categ::ZERO => self.0.trailing_ones() as usize,
            Categ::ONE => self.0.trailing_zeros() as usize,
        }
    }

    /// Revert categ value for all elements
    pub fn revert(&mut self, nb_elem: usize) {
        self.0 = !self.0 & ((1 << nb_elem) - 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let mut bitmap = BitmapCateg::default();
        // Default is all zeros
        assert!(matches!(bitmap.get(0), Categ::ZERO));
        assert!(matches!(bitmap.get(5), Categ::ZERO));

        bitmap.set(0, Categ::ONE);
        assert!(matches!(bitmap.get(0), Categ::ONE));
        assert!(matches!(bitmap.get(1), Categ::ZERO));

        bitmap.set(31, Categ::ONE);
        assert!(matches!(bitmap.get(31), Categ::ONE));

        bitmap.set(0, Categ::ZERO);
        assert!(matches!(bitmap.get(0), Categ::ZERO));
    }

    #[test]
    fn test_get_first() {
        let mut bitmap = BitmapCateg::default();
        // All zeros: first ZERO is index 0, first ONE is 32 (u32::BITS)
        assert_eq!(bitmap.get_first(Categ::ZERO), 0);
        assert_eq!(bitmap.get_first(Categ::ONE), 32);

        bitmap.set(0, Categ::ONE);
        // 00...01: first ZERO is index 1, first ONE is index 0
        assert_eq!(bitmap.get_first(Categ::ZERO), 1);
        assert_eq!(bitmap.get_first(Categ::ONE), 0);

        bitmap.set(1, Categ::ONE);
        // 00...11: first ZERO is index 2, first ONE is index 0
        assert_eq!(bitmap.get_first(Categ::ZERO), 2);
        assert_eq!(bitmap.get_first(Categ::ONE), 0);

        bitmap.set(0, Categ::ZERO);
        // 00...10: first ZERO is index 0, first ONE is index 1
        assert_eq!(bitmap.get_first(Categ::ZERO), 0);
        assert_eq!(bitmap.get_first(Categ::ONE), 1);
    }

    #[test]
    fn test_revert() {
        let mut bitmap = BitmapCateg::default();
        bitmap.set(0, Categ::ONE);
        bitmap.set(2, Categ::ONE);
        // State: ...00101 (binary)

        bitmap.revert(3);
        // Masking with 3 bits (111) and flipping: ...00010
        assert!(matches!(bitmap.get(0), Categ::ZERO));
        assert!(matches!(bitmap.get(1), Categ::ONE));
        assert!(matches!(bitmap.get(2), Categ::ZERO));
        // Bits beyond nb_elem should be zero
        assert!(matches!(bitmap.get(3), Categ::ZERO));
    }
}
