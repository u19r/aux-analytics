use crate::constants::{FNV_OFFSET_BASIS, FNV_PRIME};

/// Hash the ordered projected fields, preserving nulls and field boundaries.
pub(super) struct CheckRowHash(u64);

impl CheckRowHash {
    pub(super) fn new() -> Self {
        Self(FNV_OFFSET_BASIS)
    }

    pub(super) fn push_field(&mut self, value: Option<&str>) {
        let Some(value) = value else {
            self.push_bytes(&[0]);
            return;
        };
        self.push_bytes(&[1]);
        self.push_bytes(&(value.len() as u64).to_le_bytes());
        self.push_bytes(value.as_bytes());
    }

    pub(super) fn finish(self) -> u64 {
        self.0
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(FNV_PRIME);
        }
    }
}
