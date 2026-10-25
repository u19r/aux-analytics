use super::metric_points_manifest;

#[test]
fn given_metric_point_fixture_when_manifest_is_validated_then_typed_layout_columns_are_preserved() {
    let manifest = metric_points_manifest();

    manifest
        .validate()
        .expect("metric point fixture manifest should validate");
    let metric_table = manifest
        .tables
        .iter()
        .find(|table| table.analytics_table_name == "metric_points_v1")
        .expect("metric points table exists");
    assert!(
        metric_table
            .columns
            .iter()
            .any(|column| column.column_name == "series_name")
    );
    assert!(
        metric_table
            .columns
            .iter()
            .any(|column| column.column_name == "value_decimal")
    );
    assert_eq!(metric_table.partition_keys[0].column_name, "tenant_id");
}
