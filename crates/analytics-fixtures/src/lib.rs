mod sample_records;

pub use sample_records::{
    billing_rate_class_map_table, generic_items_manifest, legacy_item, metric_points_manifest,
    metric_points_table, storage_key, user_item, users_manifest,
};

#[cfg(test)]
mod sample_records_tests;
