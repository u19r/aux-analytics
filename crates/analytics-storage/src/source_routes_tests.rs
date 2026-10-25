use analytics_contract::TableRegistration;
use config::AnalyticsStreamType;
use serde_json::json;

use crate::{planning::SourceTablePlan, source_routes::CompiledSourceRoutes};

#[test]
fn given_aux_storage_plan_when_routes_are_compiled_then_plan_source_is_registered() {
    let plan =
        SourceTablePlan::aux_storage("orders_stream".to_string(), vec!["orders".to_string()]);

    let routes = CompiledSourceRoutes::from_plans(&[plan]).expect("compile routes");

    assert!(routes.contains_source("orders_stream"));
}

#[test]
fn given_registered_source_plan_when_routes_are_compiled_then_registration_source_is_registered() {
    let plan = SourceTablePlan {
        source_table_name: "tenant_orders".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["orders".to_string()],
        registrations: vec![registration("tenant_orders", Some("tenant_"), "orders")],
        stream_type: AnalyticsStreamType::AuxStorage,
        stream_identifier: None,
    };

    let routes = CompiledSourceRoutes::from_plans(&[plan]).expect("compile routes");

    assert!(routes.contains_source("tenant_orders"));
    assert!(routes.contains_source("tenant_customers"));
    assert!(!routes.contains_source("customer_orders"));
}

#[test]
fn given_multiple_registrations_for_one_source_when_routes_are_compiled_then_all_tables_are_routed()
{
    let plan = SourceTablePlan {
        source_table_name: "tenant_orders".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["orders".to_string(), "order_events".to_string()],
        registrations: vec![
            registration("tenant_orders", None, "orders"),
            registration("tenant_orders", None, "order_events"),
        ],
        stream_type: AnalyticsStreamType::AuxStorage,
        stream_identifier: None,
    };

    let routes = CompiledSourceRoutes::from_plans(&[plan]).expect("compile routes");

    let tables = routes
        .analytics_tables_for_source("tenant_orders")
        .expect("route source");
    assert_eq!(tables, ["orders", "order_events"]);
}

#[test]
fn given_exact_and_prefix_plans_when_source_is_matched_then_only_registered_sources_match() {
    let exact =
        SourceTablePlan::aux_storage("orders_stream".to_string(), vec!["orders".to_string()]);
    let prefix = prefixed_plan();
    let routes = CompiledSourceRoutes::from_plans(&[exact, prefix]).expect("compile routes");

    assert!(routes.contains_source("orders_stream"));
    assert!(routes.contains_source("tenant_orders"));
    assert!(routes.contains_source("tenant_customers"));
    assert!(!routes.contains_source("order_stream"));
    assert!(!routes.contains_source("customers"));
}

#[test]
fn given_duplicate_and_overlapping_plans_when_routes_are_compiled_then_membership_is_order_independent()
 {
    let exact =
        SourceTablePlan::aux_storage("tenant_orders".to_string(), vec!["orders".to_string()]);
    let prefix = prefixed_plan();
    let routes_with_duplicates =
        CompiledSourceRoutes::from_plans(&[exact.clone(), prefix.clone(), exact])
            .expect("compile routes");
    let routes_reordered = CompiledSourceRoutes::from_plans(&[
        prefix,
        SourceTablePlan::aux_storage("tenant_orders".to_string(), vec!["orders".to_string()]),
    ])
    .expect("compile routes");

    for source_table_name in ["tenant_orders", "tenant_customers", "customers"] {
        assert_eq!(
            routes_with_duplicates.contains_source(source_table_name),
            routes_reordered.contains_source(source_table_name),
            "source membership changed for {source_table_name}"
        );
    }
}

#[test]
fn given_empty_or_unregistered_plans_when_routes_are_compiled_then_no_source_is_fabricated() {
    let empty_routes = CompiledSourceRoutes::from_plans(&[]).expect("compile routes");
    assert!(!empty_routes.contains_source("orders_stream"));

    let empty_plan = SourceTablePlan::aux_storage("unused_stream".to_string(), Vec::new());
    let routes = CompiledSourceRoutes::from_plans(&[empty_plan]).expect("compile routes");

    assert!(!routes.contains_source("unused_stream"));
    assert!(!routes.contains_source("unregistered_stream"));
    assert!(
        routes
            .analytics_tables_for_source("unregistered_stream")
            .expect("unregistered global stream source")
            .is_empty()
    );
}

#[test]
fn given_third_overlapping_route_when_it_adds_a_table_then_routing_fails_closed() {
    let first = prefixed_plan();
    let second = first.clone();
    let mut third = first.clone();
    third.analytics_table_names = vec!["other_rows".to_string()];
    let routes = CompiledSourceRoutes::from_plans(&[first, second, third]).unwrap();

    let error = routes
        .analytics_tables_for_source("tenant_orders")
        .expect_err("every overlapping route must be checked");

    assert_eq!(error.category(), "ambiguous_source_route");
}

#[test]
#[ignore = "run alone with --test-threads=1 for allocation isolation"]
fn given_compiled_routes_when_records_are_routed_then_lookup_does_not_allocate() {
    let routes = CompiledSourceRoutes::from_plans(&[prefixed_plan()]).unwrap();
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "route_lookup",
        file!(),
        line!(),
        None,
    );
    for _ in 0..1_000 {
        let tables = routes
            .analytics_tables_for_source(std::hint::black_box("tenant_orders"))
            .unwrap();
        assert_eq!(std::hint::black_box(tables).len(), 1);
        assert!(
            routes
                .analytics_tables_for_source(std::hint::black_box("job"))
                .unwrap()
                .is_empty()
        );
    }
    let report = guard.finish();

    assert_eq!(report.allocation_count, 0);
    assert_eq!(report.allocated_bytes, 0);
}

#[test]
fn given_subset_overlapping_routes_when_order_changes_then_routing_always_fails_closed() {
    let subset = prefixed_plan();
    let mut superset = subset.clone();
    superset.analytics_table_names.push("events".to_string());
    for plans in [
        [subset.clone(), superset.clone()],
        [superset.clone(), subset.clone()],
    ] {
        let routes = CompiledSourceRoutes::from_plans(&plans).unwrap();
        assert_eq!(
            routes
                .analytics_tables_for_source("tenant_orders")
                .expect_err("overlapping destinations differ")
                .category(),
            "ambiguous_source_route"
        );
    }
}

#[test]
fn given_equal_overlapping_route_sets_when_order_changes_then_routing_preserves_first_route_order()
{
    let mut first = prefixed_plan();
    first.analytics_table_names.push("events".to_string());
    let mut second = first.clone();
    second.analytics_table_names.reverse();
    let routes = CompiledSourceRoutes::from_plans(&[first.clone(), second]).unwrap();

    assert_eq!(
        routes.analytics_tables_for_source("tenant_orders").unwrap(),
        first.analytics_table_names
    );
}

fn prefixed_plan() -> SourceTablePlan {
    SourceTablePlan {
        source_table_name: "tenant_sources".to_string(),
        source_table_name_prefix: Some("tenant_".to_string()),
        analytics_table_names: vec!["tenant_rows".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::AuxStorage,
        stream_identifier: None,
    }
}

fn registration(
    source_table_name: &str,
    source_table_name_prefix: Option<&str>,
    analytics_table_name: &str,
) -> TableRegistration {
    serde_json::from_value(json!({
        "source_table_name": source_table_name,
        "source_table_name_prefix": source_table_name_prefix,
        "analytics_table_name": analytics_table_name,
    }))
    .expect("registration")
}
