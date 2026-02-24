//! # Manage config option providing a memory size
//!
//! The memory size can be defined with an optional suffix, like 64k or 8mb.
//!
//! The suffix is always converted using binary conversion (1024 and not 1000), as it is used for buffer allocation.
//! So 64k = 64ki = 64kb = 64kib = 65536

use std::str::FromStr;

use anyhow::anyhow;
use serde::{Deserialize, de::Error};

#[derive(Default, PartialEq, Debug, Clone)]
pub struct MemSize(usize);

impl MemSize {
    /// Get memory size as number of bytes
    #[must_use]
    pub fn as_nb_bytes(&self) -> usize {
        self.0
    }
}

impl MemSize {
    #[must_use]
    pub fn new(n: usize) -> Self {
        Self(n)
    }
}

impl<'de> Deserialize<'de> for MemSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        MemSize::from_str(&s).map_err(D::Error::custom)
    }
}

/// `chedked_shl`() only checks if bit shift parameter is bigger than integer storage
/// It does not detect overflow.
/// Solution from: [](https://stackoverflow.com/a/63538027)
fn safe_shl<T>(n: T, shift_for: u32) -> Option<T>
where
    T: Default + Eq,
    for<'a> &'a T: std::ops::Shl<u32, Output = T> + std::ops::Shr<u32, Output = T>,
{
    #[allow(clippy::cast_possible_truncation)]
    let bits_in_t = std::mem::size_of::<T>() as u32 * 8;
    let zero = T::default();
    if &n >> (bits_in_t - shift_for) != zero {
        return None; // would lose some data
    }
    Some(&n << shift_for)
}

impl FromStr for MemSize {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut ch = s.chars().peekable();

        let mut si_unit = 0;
        let mut value = 0usize;

        // consume all digits
        while let Some(c) = ch.peek() {
            match *c {
                '0'..='9' => {
                    value = value
                        .checked_mul(10)
                        .ok_or_else(|| anyhow!("MemSize parsing: overflow"))?;

                    value = value
                        .checked_add(*c as usize - '0' as usize)
                        .ok_or_else(|| anyhow!("MemSize parsing: overflow"))?;
                }
                ' ' => (),  // ignore spaces
                _ => break, // stop on any other char
            }
            let _ = ch.next();
        }

        // consume any SI prefix
        if let Some(c) = ch.peek() {
            match *c {
                'k' | 'K' => si_unit = 1,
                'm' | 'M' => si_unit = 2,
                'g' | 'G' => si_unit = 3,
                't' | 'T' => si_unit = 4,
                'p' | 'P' => si_unit = 5,
                'e' | 'E' => si_unit = 6,
                _ => (),
            }
            if si_unit != 0 {
                let _ = ch.next();
            }
        }

        // consume any 'i' indication (kibi vs kilo, binary prefixes)
        if let Some(c) = ch.peek()
            && (*c == 'i' || *c == 'I')
        {
            let _ = ch.next();
        }

        // consume any trailing b
        if let Some(c) = ch.peek()
            && (*c == 'b' || *c == 'B')
        {
            let _ = ch.next();
        }

        // expecting no more chars
        anyhow::ensure!(
            ch.next().is_none(),
            "MemSize parsing: invalid chars are remaining"
        );

        // compute final value
        if si_unit != 0 {
            value = safe_shl(value, 10 * si_unit)
                .ok_or_else(|| anyhow!("MemSize parsing: overflow"))?;
        }

        Ok(Self(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok() {
        for (input, expected) in [
            ("0", 0),
            ("42", 42),
            ("16k", 16 * (1 << 10)),
            ("8MiB", 8 * (1 << 20)),
            ("1 gb", 1 << 30),
            ("2TB", 2 * (1 << 40)),
            ("12p", 12 * (1 << 50)),
            ("8eib", 8 * (1 << 60)),
            ("18446744073709551615", 18_446_744_073_709_551_615),
        ] {
            assert_eq!(MemSize::from_str(input).unwrap().as_nb_bytes(), expected);
        }
    }

    fn parse_invalid_str() {
        for input in [
            "x",                     // invalid string
            "10€",                   // invalid unit with multi-byte unicode
            "16mm",                  // invalid unit
            "18446744073709551616",  // overflow by addition
            "184467440737095516150", // overflow by multiplication
            "16eib",                 // overflow by SI unit
        ] {
            assert!(MemSize::from_str(input).is_err());
        }
    }
}
