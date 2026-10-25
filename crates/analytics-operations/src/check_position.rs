use std::{cmp::Ordering, fmt, io};

use analytics_contract::SourcePosition;
use serde::{Deserialize, Serialize};
use storage_types::{SystemStreamPosition, SystemStreamScope};

use crate::constants::{FNV_OFFSET_BASIS, FNV_PRIME};

/// A check carries the canonical provider position or explicit absence.
/// Two absent positions allow a content-only comparison; a positioned row
/// cannot establish progress against an unpositioned row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckSourcePosition(Option<SourcePosition>);

impl CheckSourcePosition {
    #[must_use]
    pub fn system_stream(source_scope: SystemStreamScope, position: SystemStreamPosition) -> Self {
        Self(Some(SourcePosition::SystemStream {
            source_scope,
            position,
        }))
    }

    pub fn from_source_position(value: SourcePosition) -> Result<Self, String> {
        value.validate().map_err(|error| error.to_string())?;
        Ok(Self(Some(value)))
    }

    pub fn parse_source_position(value: &str) -> Result<Self, String> {
        serde_json::from_str(value).map_err(|error| error.to_string())
    }

    #[must_use]
    pub fn partial_cmp_position(&self, other: &Self) -> Option<Ordering> {
        match (&self.0, &other.0) {
            (Some(left), Some(right)) => left.partial_cmp_position(right),
            (None, None) => Some(Ordering::Equal),
            _ => None,
        }
    }

    /// Serialize directly into the hash: retain every lane and offset byte
    /// without allocating an intermediate JSON string.
    #[must_use]
    pub fn hash_seed(&self) -> u64 {
        let mut hash = PositionHash(FNV_OFFSET_BASIS);
        serde_json::to_writer(&mut hash, self).expect(
            "source positions contain only JSON-compatible fields and the hash writer cannot fail",
        );
        hash.0
    }
}

impl fmt::Display for CheckSourcePosition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        serde_json::to_string(self)
            .map_err(|_| fmt::Error)
            .and_then(|value| formatter.write_str(&value))
    }
}

struct PositionHash(u64);

impl io::Write for PositionHash {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(FNV_PRIME);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
