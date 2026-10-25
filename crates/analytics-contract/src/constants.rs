// Manifest wire version and default mapping.
pub const MANIFEST_VERSION: u32 = 1;

pub const DEFAULT_DOCUMENT_COLUMN: &str = "item";
pub const DEFAULT_PARTITION_KEY_ATTRIBUTE_NAME: &str = "pk";

// Reserved analytical output columns.
pub const BUILTIN_TENANT_ID_COLUMN: &str = "tenant_id";
pub const BUILTIN_ROW_ID_COLUMN: &str = "__id";
pub const BUILTIN_TABLE_NAME_COLUMN: &str = "table_name";
pub const INTERNAL_INGESTED_AT_COLUMN: &str = "__ingested_at";
pub const INTERNAL_EXPIRY_COLUMN: &str = "__expiry";
pub const INTERNAL_MISSING_RETENTION_COLUMN: &str = "__missing_retention";

// Structured query resource bounds.
pub(crate) const MAX_STRUCTURED_QUERY_JOINS: usize = 2;
pub(crate) const MAX_CONDITIONAL_BRANCHES: usize = 32;
pub(crate) const MAX_CONDITIONAL_COMPARISONS: usize = 16;
pub(crate) const MAX_DOCUMENT_PREDICATE_BRANCHES: usize = 32;
pub(crate) const MAX_DOCUMENT_PREDICATE_DEPTH: usize = 32;
pub(crate) const MAX_DOCUMENT_PREDICATE_NODES: usize = 256;
pub(crate) const MAX_QUERY_IDENTIFIER_LEN: usize = 255;

/// Default row count used when a structured query omits `limit`.
pub const DEFAULT_STRUCTURED_QUERY_LIMIT: u32 = 1_000;
pub const MAX_STRUCTURED_QUERY_BOUND: u32 = 100_000;

// Privacy policy compilation bounds.
pub(crate) const MAX_REGEX_RULES: usize = 32;
pub(crate) const MAX_REGEX_LENGTH: usize = 512;

// Storage test fixtures and measurement bounds.
#[cfg(test)]
pub(crate) const CANONICAL_PROBE_ITERATIONS: usize = 64;

// Query canonical test fixtures and measurement bounds.
#[cfg(test)]
pub(crate) const QUERY_CANONICAL_PROBE_ITERATIONS: usize = 256;
#[cfg(test)]
pub(crate) const CANONICAL_PROBE_QUERIES: usize = 8;
#[cfg(test)]
pub(crate) const CANONICAL_PROBE_SAMPLES: usize = 3;
#[cfg(test)]
pub(crate) const CANONICAL_PROBE_WARMUPS: usize = 2;
#[cfg(test)]
pub(crate) const EXPECTED_CANONICAL_JSON: &str = r#"{"analytics_table_name":"events","filters":[{"expression":{"column_name":"payload","kind":"column"},"kind":"eq","value":{"alpha":{"array":[{"a":6,"z":7}],"nested":{"a":0,"z":1}},"zeta":[{"alpha":[true,null,{"a":"first","z":"last"}],"beta":2},{"middle":{"left":3,"right":4}}]}}],"limit":25,"offset":3,"select":[{"alias":null,"column_name":"payload","kind":"column"}]}"#;
#[cfg(test)]
pub(crate) const EXPECTED_QUERY_HASH: &str =
    "sha256:95974d6870df06b27b753f466c16b1e30621f41413eabf81693b9344b5ebc667";

// Canonical lowercase digest encoding.
pub(crate) const HEX: &[u8; 16] = b"0123456789abcdef";
