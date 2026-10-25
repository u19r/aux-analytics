// Manifest validation contract at the analytics-contract boundary.
// Actors are manifest callers and the validator; the environment supplies a
// finite manifest case. Durable output is the validation result; lastCase is
// transient input evidence. Invalid version, names, layout references,
// retention, row expansion, tenant scope, join policy, and row identity are
// explicit rejection outcomes. Time and storage publication are abstracted;
// fail-atomic publication is handed to the owning engine model.
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
    | InvalidRowExpansion
    | GlobalReferenceHasTenantScope
    | InvalidJoinPolicy
    | InvalidRowIdentity

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
    | InvalidRowExpansionManifest
    | GlobalReferenceTenantScopeManifest
    | InvalidJoinPolicyManifest
    | InvalidRowIdentityManifest
    | NotCheckedManifest

  var lastResult: ValidationResult
  var lastCase: ManifestCase

  pure val validationResults: Set[ValidationResult] = Set(
    NotChecked,
    Valid,
    UnsupportedVersion,
    DuplicateAnalyticsTableName,
    EmptyField,
    ReservedOutputColumn,
    DuplicateColumnName,
    UnknownLayoutColumn,
    InvalidRetentionPeriod,
    InvalidRowExpansion,
    GlobalReferenceHasTenantScope,
    InvalidJoinPolicy,
    InvalidRowIdentity
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
      | InvalidRowExpansionManifest => InvalidRowExpansion
      | GlobalReferenceTenantScopeManifest => GlobalReferenceHasTenantScope
      | InvalidJoinPolicyManifest => InvalidJoinPolicy
      | InvalidRowIdentityManifest => InvalidRowIdentity
      | NotCheckedManifest => NotChecked
    }

  action init =
    all {
      lastResult' = NotChecked,
      lastCase' = NotCheckedManifest,
    }

  action CheckValidSimpleManifest =
    all {
      lastResult' = validateCase(ValidSimpleManifest),
      lastCase' = ValidSimpleManifest,
    }

  action CheckValidGenericDocumentManifest =
    all {
      lastResult' = validateCase(ValidGenericDocumentManifest),
      lastCase' = ValidGenericDocumentManifest,
    }

  action CheckValidRetentionManifest =
    all {
      lastResult' = validateCase(ValidRetentionManifest),
      lastCase' = ValidRetentionManifest,
    }

  action CheckUnsupportedVersion =
    all {
      lastResult' = validateCase(UnsupportedVersionManifest),
      lastCase' = UnsupportedVersionManifest,
    }

  action CheckDuplicateAnalyticsTableName =
    all {
      lastResult' = validateCase(DuplicateAnalyticsTableManifest),
      lastCase' = DuplicateAnalyticsTableManifest,
    }

  action CheckEmptySourceTableName =
    all {
      lastResult' = validateCase(EmptySourceTableManifest),
      lastCase' = EmptySourceTableManifest,
    }

  action CheckEmptyAnalyticsTableName =
    all {
      lastResult' = validateCase(EmptyAnalyticsTableManifest),
      lastCase' = EmptyAnalyticsTableManifest,
    }

  action CheckReservedProjectionColumn =
    all {
      lastResult' = validateCase(ReservedProjectionColumnManifest),
      lastCase' = ReservedProjectionColumnManifest,
    }

  action CheckReservedInternalRetentionColumn =
    all {
      lastResult' = validateCase(ReservedInternalRetentionColumnManifest),
      lastCase' = ReservedInternalRetentionColumnManifest,
    }

  action CheckDuplicateProjectionColumn =
    all {
      lastResult' = validateCase(DuplicateProjectionColumnManifest),
      lastCase' = DuplicateProjectionColumnManifest,
    }

  action CheckUnknownPartitionColumn =
    all {
      lastResult' = validateCase(UnknownPartitionColumnManifest),
      lastCase' = UnknownPartitionColumnManifest,
    }

  action CheckZeroRetentionPeriod =
    all {
      lastResult' = validateCase(ZeroRetentionPeriodManifest),
      lastCase' = ZeroRetentionPeriodManifest,
    }

  action CheckEmptyRetentionTimestamp =
    all {
      lastResult' = validateCase(EmptyRetentionTimestampManifest),
      lastCase' = EmptyRetentionTimestampManifest,
    }

  action CheckInvalidRowExpansion = all {
    lastResult' = validateCase(InvalidRowExpansionManifest),
    lastCase' = InvalidRowExpansionManifest,
  }

  action CheckGlobalReferenceTenantScope = all {
    lastResult' = validateCase(GlobalReferenceTenantScopeManifest),
    lastCase' = GlobalReferenceTenantScopeManifest,
  }

  action CheckInvalidJoinPolicy = all {
    lastResult' = validateCase(InvalidJoinPolicyManifest),
    lastCase' = InvalidJoinPolicyManifest,
  }

  action CheckInvalidRowIdentity = all {
    lastResult' = validateCase(InvalidRowIdentityManifest),
    lastCase' = InvalidRowIdentityManifest,
  }

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
    CheckInvalidRowExpansion,
    CheckGlobalReferenceTenantScope,
    CheckInvalidJoinPolicy,
    CheckInvalidRowIdentity,
  }

  /* Properties */

  pure def isValidCase(manifest: ManifestCase): bool = or {
    manifest == ValidSimpleManifest,
    manifest == ValidGenericDocumentManifest,
    manifest == ValidRetentionManifest,
  }

  pure def isRejectedCase(manifest: ManifestCase): bool = and {
    manifest != NotCheckedManifest,
    not(isValidCase(manifest)),
  }

  val acceptedOnlyForValidCase =
    lastResult == Valid implies isValidCase(lastCase)
  val rejectedCasesNeverAccepted =
    isRejectedCase(lastCase) implies lastResult != Valid
  val resultMatchesCanonicalCaseReducer =
    lastResult == validateCase(lastCase)
  val inv = and {
    resultMatchesCanonicalCaseReducer,
    acceptedOnlyForValidCase,
    rejectedCasesNeverAccepted,
  }

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
      assert(validateCase(InvalidRowExpansionManifest) == InvalidRowExpansion),
      assert(validateCase(GlobalReferenceTenantScopeManifest) == GlobalReferenceHasTenantScope),
      assert(validateCase(InvalidJoinPolicyManifest) == InvalidJoinPolicy),
      assert(validateCase(InvalidRowIdentityManifest) == InvalidRowIdentity),
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
