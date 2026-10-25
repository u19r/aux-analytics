use super::{TrimError, TrimPredicate, TrimPredicateValue};

#[test]
fn given_nested_predicates_when_compiled_then_sql_and_bound_values_preserve_order() {
    let predicate = TrimPredicate::All {
        predicates: vec![
            TrimPredicate::Eq {
                column: "name".into(),
                value: TrimPredicateValue::Text("x' OR TRUE --".into()),
            },
            TrimPredicate::Any {
                predicates: vec![
                    TrimPredicate::Gte {
                        column: "version".into(),
                        value: TrimPredicateValue::Integer(7),
                    },
                    TrimPredicate::IsNull {
                        column: "expiry".into(),
                    },
                    TrimPredicate::NotEq {
                        column: "active".into(),
                        value: TrimPredicateValue::Boolean(false),
                    },
                ],
            },
        ],
    };
    let mut values = Vec::new();
    let sql = predicate.compile(&mut values).unwrap();
    assert_eq!(
        sql,
        "(\"name\" = ? AND (\"version\" >= ? OR \"expiry\" IS NULL OR \"active\" <> ?))"
    );
    assert_eq!(
        values,
        vec![
            duckdb::types::Value::Text("x' OR TRUE --".into()),
            duckdb::types::Value::BigInt(7),
            duckdb::types::Value::Boolean(false),
        ]
    );
}

#[test]
fn given_invalid_nested_column_when_compiled_then_existing_parameters_remain_unchanged() {
    let predicate = TrimPredicate::All {
        predicates: vec![
            TrimPredicate::Eq {
                column: "name".into(),
                value: TrimPredicateValue::Text("valid".into()),
            },
            TrimPredicate::IsNull {
                column: "expiry; DROP TABLE records".into(),
            },
        ],
    };
    let mut values = vec![duckdb::types::Value::BigInt(19)];
    assert!(matches!(
        predicate.compile(&mut values),
        Err(TrimError::UnsafePredicate)
    ));
    assert_eq!(values, vec![duckdb::types::Value::BigInt(19)]);
}
