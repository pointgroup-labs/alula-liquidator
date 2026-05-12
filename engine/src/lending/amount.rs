//! Typed amount newtypes for protocol values.
//!
//! These wrap `i128` to give strategies a typed API and prevent mixing
//! j-tokens, d-tokens, and underlying token amounts.

use serde::{Deserialize, Serialize};

macro_rules! amount_newtype {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name(pub i128);

        impl $name {
            pub const ZERO: Self = Self(0);

            pub fn raw(self) -> i128 {
                self.0
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
    };
}

amount_newtype!(JToken);
amount_newtype!(DToken);
amount_newtype!(Underlying);
