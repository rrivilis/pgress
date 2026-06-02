//! Unique identifiers for graph elements. Wraps uuid v4.

use uuid::Uuid;

pub type Uid = Uuid;

/// Generate a fresh random UID.
#[inline]
pub fn fresh() -> Uid {
    Uuid::new_v4()
}

/// A nil UID for use as a sentinel (not a valid element ID).
pub const NIL: Uid = Uuid::nil();
