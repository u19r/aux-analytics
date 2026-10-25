use super::bdd::{parse_feature, parse_tags};

#[test]
fn given_comma_separated_tags_when_parsed_then_whitespace_is_normalized() {
    assert_eq!(
        parse_tags("@F101_S001, @F101_S002").expect("tags should parse"),
        vec!["@F101_S001", "@F101_S002"]
    );
}

#[test]
fn given_feature_tag_when_parsed_then_feature_scope_is_not_a_scenario() {
    let catalog = parse_feature("@F101\nFeature: analytics\n").expect("feature");
    assert_eq!(catalog.declaration("@F101"), Some((1, false)));
}
