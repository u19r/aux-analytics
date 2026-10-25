use super::*;
use crate::sql::source_position_column_statement;

#[test]
#[ignore = "debug-only source-position allocation regression"]
fn given_source_positions_when_sql_literals_are_measured_then_outputs_match_and_a_cpu_or_allocation_win_is_measured()
 {
    let workload = super::realistic_workload(super::REALISTIC_RECORD_COUNT);
    println!(
        "source-position-fixture | records={} | attributes={} | key_bytes=100 | \
         payload_bytes=1200 | gsi_families=2 | ttl=true | query_sizes=1,25,100,500 | \
         paired_samples={}",
        workload.records.len(),
        super::REALISTIC_ATTRIBUTE_COUNT,
        super::PRODUCTION_PAIRED_SAMPLES,
    );

    let result = super::measure_production_candidate(
        "source position column literal",
        "sql.rs::source_position_column_statement::fixed_column_literal",
        || source_position_probe(false),
        || source_position_probe(true),
    );
    super::print_production_candidate_results(
        "source-position-candidate",
        std::slice::from_ref(&result),
    );
    assert_eq!(result.name, "source position column literal");
    assert!(result.kept);
    assert_eq!(result.serialized_bytes, 85_000);
}

fn source_position_probe(production: bool) -> ProductionProbe {
    super::combine_probes((0..super::REALISTIC_RECORD_COUNT).map(|index| {
        let table_name = format!("analytics_items_{index:04}", index = index % 32);
        let statement = if production {
            source_position_column_statement(table_name.as_str())
        } else {
            baseline_source_position_statement(table_name.as_str())
        };
        ProductionProbe {
            output_digest: super::digest_bytes(statement.as_bytes()),
            serialized_bytes: statement.len() as u64,
        }
    }))
}

fn baseline_source_position_statement(table_name: &str) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} JSON;",
        super::quote_identifier(table_name),
        super::quote_identifier("__source_position")
    )
}
