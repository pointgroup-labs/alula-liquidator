//! Constants, fixed-point multiplication helpers and type definitions for protocol fixed-point math.

use super::error::LMError;
use crate::lending_model::error::MapArithmeticError;
use serde::{Deserialize, Serialize};

pub const BPS_FACTOR: i128 = 10_000;

macro_rules! amount_newtype {
    ($name:ident) => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        pub struct $name(pub i128);

        impl $name {
            pub const ZERO: Self = Self(0);
            pub const BPS_FACTOR: Self = Self(BPS_FACTOR);

            pub fn checked_add(self, other: Self) -> Result<Self, LMError> {
                let res = self.0.checked_add(other.0).m_ou()?;

                Ok($name(res))
            }

            pub fn checked_sub(self, other: Self) -> Result<Self, LMError> {
                let res = self.0.checked_sub(other.0).m_ou()?;

                Ok($name(res))
            }

            pub fn checked_mul(self, other: i128) -> Result<i128, LMError> {
                self.0.checked_mul(other).m_ou()
            }

            pub fn checked_div_floor(self, other: i128) -> Result<i128, LMError> {
                self.0.checked_div(other).m_ou()
            }

            pub fn checked_div_ceil(self, other: i128) -> Result<i128, LMError> {
                let denominator = other;
                let numerator = self.0.checked_add(denominator - 1).m_ou()?;

                numerator.checked_div(denominator).m_ou()
            }

            pub fn scale_with_bps_floor(self, bps: i128) -> Result<Self, LMError> {
                let res = self.0.checked_mul(bps).m_ou()? / BPS_FACTOR;

                Ok($name(res))
            }

            pub fn scale_with_bps_ceil(self, bps: i128) -> Result<Self, LMError> {
                let numerator = self.0.checked_mul(bps).m_ou()?;
                let numerator_adjusted = numerator.checked_add(BPS_FACTOR - 1).m_ou()?;
                let denominator = BPS_FACTOR;

                let res = numerator_adjusted.checked_div(denominator).m_ou()?;

                Ok($name(res))
            }

            pub fn bps_fixed_div_ceil(self, val: i128) -> Result<i128, LMError> {
                bps_fixed_div_ceil(self.0, val).m_ou()
            }

            pub fn bps_fixed_div_floor(self, val: i128) -> Result<i128, LMError> {
                bps_fixed_div_floor(self.0, val).m_ou()
            }
        }

        impl std::ops::Add for $name {
            type Output = Self;
            fn add(self, rhs: Self) -> Self {
                Self(self.0 + rhs.0)
            }
        }

        impl std::ops::Sub for $name {
            type Output = Self;
            fn sub(self, rhs: Self) -> Self {
                Self(self.0 - rhs.0)
            }
        }

        impl From<i128> for $name {
            fn from(x: i128) -> $name {
                $name(x)
            }
        }

        impl From<$name> for i128 {
            fn from(x: $name) -> i128 {
                x.0
            }
        }
    };
}

amount_newtype!(JTokens);
amount_newtype!(DTokens);
amount_newtype!(Underlying);

pub fn bps_fixed_div_ceil(a: i128, other: i128) -> Option<i128> {
    let denominator = other;

    let numerator = a.checked_mul(BPS_FACTOR)?;
    let numerator_adjusted = numerator.checked_add(denominator - 1)?;

    Some(numerator_adjusted / denominator)
}

pub fn bps_fixed_div_floor(a: i128, other: i128) -> Option<i128> {
    let denominator = other;
    let numerator = a.checked_mul(BPS_FACTOR)?;

    Some(numerator / denominator)
}
