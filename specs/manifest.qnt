module manifest {
  type ValidationResult =
    | NotChecked
    | Valid
    | UnsupportedVersion
    | DuplicateAnalyticsTableName
    | EmptyField
    | ReservedOutputColumn
    | DuplicateColumnName
    | UnknownLayoutColumn
    | InvalidRetentionPeriod

  type ManifestCase =
    | ValidSimpleManifest
    | ValidGenericDocumentManifest
    | ValidRetentionManifest
    | UnsupportedVersionManifest
    | DuplicateAnalyticsTableManifest
    | EmptySourceTableManifest
    | EmptyAnalyticsTableManifest
    | ReservedProjectionColumnManifest
    | ReservedInternalRetentionColumnManifest
    | DuplicateProjectionColumnManifest
    | UnknownPartitionColumnManifest
    | ZeroRetentionPeriodManifest
    | EmptyRetentionTimestampManifest

  var lastResult: ValidationResult

  pure val validationResults: Set[ValidationResult] = Set(
    NotChecked,
    Valid,
    UnsupportedVersion,
    DuplicateAnalyticsTableName,
    EmptyField,
    ReservedOutputColumn,
    DuplicateColumnName,
    UnknownLayoutColumn,
    InvalidRetentionPeriod
  )

  pure def validateCase(manifest: ManifestCase): ValidationResult =
    match manifest {
      | ValidSimpleManifest => Valid
      | ValidGenericDocumentManifest => Valid
      | ValidRetentionManifest => Valid
      | UnsupportedVersionManifest => UnsupportedVersion
      | DuplicateAnalyticsTableManifest => DuplicateAnalyticsTableName
      | EmptySourceTableManifest => EmptyField
      | EmptyAnalyticsTableManifest => EmptyField
      | ReservedProjectionColumnManifest => ReservedOutputColumn
      | ReservedInternalRetentionColumnManifest => ReservedOutputColumn
      | DuplicateProjectionColumnManifest => DuplicateColumnName
      | UnknownPartitionColumnManifest => UnknownLayoutColumn
      | ZeroRetentionPeriodManifest => InvalidRetentionPeriod
      | EmptyRetentionTimestampManifest => EmptyField
    }

  action init =
    lastResult' = NotChecked

  action CheckValidSimpleManifest =
    lastResult' = validateCase(ValidSimpleManifest)

  action CheckValidGenericDocumentManifest =
    lastResult' = validateCase(ValidGenericDocumentManifest)

  action CheckValidRetentionManifest =
    lastResult' = validateCase(ValidRetentionManifest)

  action CheckUnsupportedVersion =
    lastResult' = validateCase(UnsupportedVersionManifest)

  action CheckDuplicateAnalyticsTableName =
    lastResult' = validateCase(DuplicateAnalyticsTableManifest)

  action CheckEmptySourceTableName =
    lastResult' = validateCase(EmptySourceTableManifest)

  action CheckEmptyAnalyticsTableName =
    lastResult' = validateCase(EmptyAnalyticsTableManifest)

  action CheckReservedProjectionColumn =
    lastResult' = validateCase(ReservedProjectionColumnManifest)

  action CheckReservedInternalRetentionColumn =
    lastResult' = validateCase(ReservedInternalRetentionColumnManifest)

  action CheckDuplicateProjectionColumn =
    lastResult' = validateCase(DuplicateProjectionColumnManifest)

  action CheckUnknownPartitionColumn =
    lastResult' = validateCase(UnknownPartitionColumnManifest)

  action CheckZeroRetentionPeriod =
    lastResult' = validateCase(ZeroRetentionPeriodManifest)

  action CheckEmptyRetentionTimestamp =
    lastResult' = validateCase(EmptyRetentionTimestampManifest)

  action step = any {
    CheckValidSimpleManifest,
    CheckValidGenericDocumentManifest,
    CheckValidRetentionManifest,
    CheckUnsupportedVersion,
    CheckDuplicateAnalyticsTableName,
    CheckEmptySourceTableName,
    CheckEmptyAnalyticsTableName,
    CheckReservedProjectionColumn,
    CheckReservedInternalRetentionColumn,
    CheckDuplicateProjectionColumn,
    CheckUnknownPartitionColumn,
    CheckZeroRetentionPeriod,
    CheckEmptyRetentionTimestamp,
  }

  /* Properties */

  val resultIsKnown = validationResults.contains(lastResult)
  val inv = resultIsKnown

  /* Example Runs */

  run acceptsValidManifestTest =
    all {
      assert(validateCase(ValidSimpleManifest) == Valid),
      assert(validateCase(ValidGenericDocumentManifest) == Valid),
      assert(validateCase(ValidRetentionManifest) == Valid),
    }

  run rejectsDuplicateAnalyticsTableNameTest =
    assert(validateCase(DuplicateAnalyticsTableManifest) == DuplicateAnalyticsTableName)

  run rejectsReservedColumnTest =
    all {
      assert(validateCase(ReservedProjectionColumnManifest) == ReservedOutputColumn),
      assert(validateCase(ReservedInternalRetentionColumnManifest) == ReservedOutputColumn),
    }

  run rejectsInvalidRetentionTest =
    all {
      assert(validateCase(ZeroRetentionPeriodManifest) == InvalidRetentionPeriod),
      assert(validateCase(EmptyRetentionTimestampManifest) == EmptyField),
    }

  run rejectsMalformedTableAndLayoutFieldsTest =
    all {
      assert(validateCase(UnsupportedVersionManifest) == UnsupportedVersion),
      assert(validateCase(EmptySourceTableManifest) == EmptyField),
      assert(validateCase(EmptyAnalyticsTableManifest) == EmptyField),
      assert(validateCase(DuplicateProjectionColumnManifest) == DuplicateColumnName),
      assert(validateCase(UnknownPartitionColumnManifest) == UnknownLayoutColumn),
    }

  /* Witnesses */

  val valid_manifest_witness = lastResult == Valid
  val invalid_manifest_witness = and {
    lastResult != Valid,
    lastResult != NotChecked,
  }
  val retention_error_witness = or {
    lastResult == InvalidRetentionPeriod,
    lastResult == EmptyField,
  }
}
