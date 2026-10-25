use super::symbols::{CodeToken, DeclarationRole, SourceSymbols};

#[test]
fn given_literals_comments_and_unicode_when_lexed_then_only_borrowed_code_symbols_have_offsets() {
    let source = r###"module real {
        /* outer /* action fake = true */ café */
        val quoted = "action fake = true"
        val raw = r#"action fake = true"#
        val bytes = b"action fake = true"
        val raw_bytes = br#"action fake = true"#
        val character = 'é'
        action step = state' = 1
        val inv = state >= 0
    }"###;
    let symbols = SourceSymbols::parse(source);
    assert!(!symbols.has_identifier("fake"));
    assert!(symbols.has_model_symbol("action", "step"));
    assert!(symbols.has_model_value("inv"));
    for token in &symbols.tokens {
        if let CodeToken::Identifier(name, offset) = token {
            assert_eq!(source.get(*offset..*offset + name.len()), Some(*name));
            assert_eq!(
                name.as_ptr(),
                source
                    .get(*offset..)
                    .expect("token offset is a character boundary")
                    .as_ptr()
            );
        }
    }
    let declaration = symbols
        .find_declaration_at(source.find("state' = 1").expect("target"))
        .expect("action");
    assert_eq!(declaration.name, "step");
    assert_eq!(declaration.role, DeclarationRole::Behavior);
}

#[test]
fn given_nested_local_declarations_when_mutation_target_is_bound_then_module_owner_is_preserved() {
    let source = "module real { action step = { val local = 1; state' = local } val inv = true }";
    let symbols = SourceSymbols::parse(source);
    let declaration = symbols
        .find_declaration_at(source.find("state' =").expect("target"))
        .expect("owner");
    assert_eq!(declaration.name, "step");
    assert_eq!(declaration.role, DeclarationRole::Behavior);
}
