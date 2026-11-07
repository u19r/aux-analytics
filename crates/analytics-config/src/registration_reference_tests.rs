use serde_json::json;

use crate::RegistrationReference;

#[test]
fn given_an_immutable_reference_when_parsed_then_only_complete_versioned_source_bound_inputs_are_accepted() {
    let valid = json!({"schema_version":1,"region":"us-east-1","bucket":"registration-fixture","key":"registrations/document.json","version":"immutable-v1","sha256":"a".repeat(64),"bytes":98305,"source":{"repository":"example/producer","revision":"b".repeat(40),"tree":"c".repeat(40)}});
    assert!(RegistrationReference::try_from(valid.to_string().as_str()).is_ok());
    for (field, value) in [("schema_version",json!(2)),("version",json!("null")),("version",json!("")),("bytes",json!(0)),("sha256",json!("latest")),("bucket",json!("")),("region",json!("")),("key",json!(""))] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(RegistrationReference::try_from(invalid.to_string().as_str()).is_err(), "{field}");
    }
    let mut invalid = valid.clone();
    invalid["source"]["revision"] = json!("main");
    assert!(RegistrationReference::try_from(invalid.to_string().as_str()).is_err());
    invalid = valid;
    invalid["unknown"] = json!(true);
    assert!(RegistrationReference::try_from(invalid.to_string().as_str()).is_err());
}
