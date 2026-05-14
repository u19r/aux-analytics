module manifest_projection_bughunt {
  var acceptedManifest: bool
  var layoutResolutionCount: int
  var reservedColumnProjected: bool
  var outputSourcesUnique: bool
  var aliasConfigurationValid: bool
  var retentionConfigurationValid: bool

  pure val bools: Set[bool] = Set(true, false)

  pure def boolCount(a: bool, b: bool, c: bool): int =
    (if (a) 1 else 0) + (if (b) 1 else 0) + (if (c) 1 else 0)

  pure def layoutResolutionSources(hasProjectionColumn: bool, hasDocumentColumn: bool, hasBuiltinColumn: bool): int =
    if (hasBuiltinColumn) 1 else boolCount(hasProjectionColumn, hasDocumentColumn, false)

  pure def reservedProjectionAllowed(columnIsReserved: bool): bool =
    not(columnIsReserved)

  pure def outputSourcesAreUnique(hasProjectionColumn: bool, hasDocumentColumn: bool): bool =
    not(hasProjectionColumn and hasDocumentColumn)

  pure def aliasesAreValid(usesAlias: bool, hasNameMap: bool, aliasIsMapped: bool): bool =
    not(usesAlias) or (hasNameMap and aliasIsMapped)

  pure def retentionIsValid(hasRetention: bool, periodPositive: bool, timestampPathNonEmpty: bool): bool =
    not(hasRetention) or (periodPositive and timestampPathNonEmpty)

  pure def manifestAccepted(
    hasProjectionColumn: bool,
    hasDocumentColumn: bool,
    hasBuiltinColumn: bool,
    columnIsReserved: bool,
    usesAlias: bool,
    hasNameMap: bool,
    aliasIsMapped: bool,
    hasRetention: bool,
    periodPositive: bool,
    timestampPathNonEmpty: bool,
  ): bool =
    layoutResolutionSources(hasProjectionColumn, hasDocumentColumn, hasBuiltinColumn) == 1
      and outputSourcesAreUnique(hasProjectionColumn, hasDocumentColumn)
      and reservedProjectionAllowed(columnIsReserved)
      and aliasesAreValid(usesAlias, hasNameMap, aliasIsMapped)
      and retentionIsValid(hasRetention, periodPositive, timestampPathNonEmpty)

  action init = all {
    acceptedManifest' = true,
    layoutResolutionCount' = 1,
    reservedColumnProjected' = false,
    outputSourcesUnique' = true,
    aliasConfigurationValid' = true,
    retentionConfigurationValid' = true,
  }

  action GenerateManifestProjectionCombination = {
    nondet has_projection_column = bools.oneOf()
    nondet has_document_column = bools.oneOf()
    nondet has_builtin_column = bools.oneOf()
    nondet column_is_reserved = bools.oneOf()
    nondet uses_alias = bools.oneOf()
    nondet has_name_map = bools.oneOf()
    nondet alias_is_mapped = bools.oneOf()
    nondet has_retention = bools.oneOf()
    nondet period_positive = bools.oneOf()
    nondet timestamp_path_non_empty = bools.oneOf()
    all {
      layoutResolutionCount' = layoutResolutionSources(has_projection_column, has_document_column, has_builtin_column),
      reservedColumnProjected' = column_is_reserved,
      outputSourcesUnique' = outputSourcesAreUnique(has_projection_column, has_document_column),
      aliasConfigurationValid' = aliasesAreValid(uses_alias, has_name_map, alias_is_mapped),
      retentionConfigurationValid' = retentionIsValid(has_retention, period_positive, timestamp_path_non_empty),
      acceptedManifest' = manifestAccepted(
        has_projection_column,
        has_document_column,
        has_builtin_column,
        column_is_reserved,
        uses_alias,
        has_name_map,
        alias_is_mapped,
        has_retention,
        period_positive,
        timestamp_path_non_empty
      ),
    }
  }

  action step = any {
    GenerateManifestProjectionCombination,
  }

  /* Properties */

  val acceptedLayoutColumnsResolveExactlyOnce =
    acceptedManifest implies layoutResolutionCount == 1

  val acceptedOutputSourcesAreUnique =
    acceptedManifest implies outputSourcesUnique

  val acceptedManifestsNeverProjectReservedColumns =
    acceptedManifest implies not(reservedColumnProjected)

  val acceptedManifestsHaveValidAliases =
    acceptedManifest implies aliasConfigurationValid

  val acceptedManifestsHaveValidRetention =
    acceptedManifest implies retentionConfigurationValid

  val inv = and {
    acceptedLayoutColumnsResolveExactlyOnce,
    acceptedOutputSourcesAreUnique,
    acceptedManifestsNeverProjectReservedColumns,
    acceptedManifestsHaveValidAliases,
    acceptedManifestsHaveValidRetention,
  }

  /* Example Runs */

  run layoutResolutionTest =
    all {
      assert(layoutResolutionSources(true, false, false) == 1),
      assert(layoutResolutionSources(true, true, false) == 2),
      assert(layoutResolutionSources(false, false, false) == 0),
    }

  run reservedProjectionAndAliasTest =
    all {
      assert(reservedProjectionAllowed(true) == false),
      assert(aliasesAreValid(true, true, true)),
      assert(aliasesAreValid(true, true, false) == false),
      assert(aliasesAreValid(false, false, false)),
    }

  run retentionValidationTest =
    all {
      assert(retentionIsValid(false, false, false)),
      assert(retentionIsValid(true, true, true)),
      assert(retentionIsValid(true, false, true) == false),
      assert(retentionIsValid(true, true, false) == false),
    }

  /* Witnesses */

  val accepted_manifest_witness = acceptedManifest
  val rejected_reserved_projection_witness = reservedColumnProjected and not(acceptedManifest)
  val ambiguous_layout_witness = layoutResolutionCount > 1 and not(acceptedManifest)
}
