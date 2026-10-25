// Projection contract for nested paths, aliases, lists, and typed failures.
// Actors are the ingestion/projection boundary and a registered table
// configuration; the environment supplies the item, path, and alias map.
// Durable output is the projected result; lastCase is transient input
// evidence. Missing paths are skipped, malformed paths and missing aliases
// are rejected, and list expansion preserves order. Storage and SQL are
// abstracted from this decision model.
module projection {
  type ProjectionResult =
    | NotChecked
    | EmailProjected
    | OrderTotalsProjected
    | MissingPathSkipped
    | EmptyAttributePath
    | InvalidAttributePath
    | MissingExpressionAttributeName
    | MissingExpressionAttributeNames

  type ProjectionCase =
    | AliasPathProjection
    | ListExpansionProjection
    | MissingPathProjection
    | EmptyAttributePathProjection
    | InvalidAttributePathProjection
    | AliasWithoutNameMapProjection
    | MissingAliasProjection
    | NotCheckedProjection

  var lastResult: ProjectionResult
  var lastCase: ProjectionCase

  pure val projectionResults: Set[ProjectionResult] = Set(
    NotChecked,
    EmailProjected,
    OrderTotalsProjected,
    MissingPathSkipped,
    EmptyAttributePath,
    InvalidAttributePath,
    MissingExpressionAttributeName,
    MissingExpressionAttributeNames
  )

  pure def projectCase(projection: ProjectionCase): ProjectionResult =
    match projection {
      | AliasPathProjection => EmailProjected
      | ListExpansionProjection => OrderTotalsProjected
      | MissingPathProjection => MissingPathSkipped
      | EmptyAttributePathProjection => EmptyAttributePath
      | InvalidAttributePathProjection => InvalidAttributePath
      | AliasWithoutNameMapProjection => MissingExpressionAttributeNames
      | MissingAliasProjection => MissingExpressionAttributeName
      | NotCheckedProjection => NotChecked
    }

  action init =
    all {
      lastResult' = NotChecked,
      lastCase' = NotCheckedProjection,
    }

  action ProjectAliasPath =
    all {
      lastResult' = projectCase(AliasPathProjection),
      lastCase' = AliasPathProjection,
    }

  action ProjectListExpansion =
    all {
      lastResult' = projectCase(ListExpansionProjection),
      lastCase' = ListExpansionProjection,
    }

  action ProjectMissingPath =
    all {
      lastResult' = projectCase(MissingPathProjection),
      lastCase' = MissingPathProjection,
    }

  action RejectEmptyAttributePath =
    all {
      lastResult' = projectCase(EmptyAttributePathProjection),
      lastCase' = EmptyAttributePathProjection,
    }

  action RejectInvalidAttributePath =
    all {
      lastResult' = projectCase(InvalidAttributePathProjection),
      lastCase' = InvalidAttributePathProjection,
    }

  action RejectAliasWithoutNameMap =
    all {
      lastResult' = projectCase(AliasWithoutNameMapProjection),
      lastCase' = AliasWithoutNameMapProjection,
    }

  action RejectMissingAlias =
    all {
      lastResult' = projectCase(MissingAliasProjection),
      lastCase' = MissingAliasProjection,
    }

  action step = any {
    ProjectAliasPath,
    ProjectListExpansion,
    ProjectMissingPath,
    RejectEmptyAttributePath,
    RejectInvalidAttributePath,
    RejectAliasWithoutNameMap,
    RejectMissingAlias,
  }

  /* Properties */

  pure def isSuccessfulCase(projection: ProjectionCase): bool = or {
    projection == AliasPathProjection,
    projection == ListExpansionProjection,
    projection == MissingPathProjection,
  }

  pure def isErrorCase(projection: ProjectionCase): bool = and {
    projection != NotCheckedProjection,
    not(isSuccessfulCase(projection)),
  }

  val successfulResultsRequireSuccessfulCases =
    (lastResult == EmailProjected or lastResult == OrderTotalsProjected or lastResult == MissingPathSkipped)
      implies isSuccessfulCase(lastCase)
  val errorCasesNeverProduceSuccess =
    isErrorCase(lastCase) implies not(
      lastResult == EmailProjected or lastResult == OrderTotalsProjected or lastResult == MissingPathSkipped
    )
  val resultMatchesCanonicalCaseReducer =
    lastResult == projectCase(lastCase)
  val inv = and {
    resultMatchesCanonicalCaseReducer,
    successfulResultsRequireSuccessfulCases,
    errorCasesNeverProduceSuccess,
  }

  /* Example Runs */

  run projectsAliasPathTest =
    assert(projectCase(AliasPathProjection) == EmailProjected)

  run expandsListChildrenTest =
    assert(projectCase(ListExpansionProjection) == OrderTotalsProjected)

  run skipsMissingPathTest =
    assert(projectCase(MissingPathProjection) == MissingPathSkipped)

  run rejectsInvalidPathTest =
    all {
      assert(projectCase(EmptyAttributePathProjection) == EmptyAttributePath),
      assert(projectCase(InvalidAttributePathProjection) == InvalidAttributePath),
    }

  run rejectsAliasMisconfigurationTest =
    all {
      assert(projectCase(AliasWithoutNameMapProjection) == MissingExpressionAttributeNames),
      assert(projectCase(MissingAliasProjection) == MissingExpressionAttributeName),
    }

  /* Witnesses */

  val projection_success_witness = or {
    lastResult == EmailProjected,
    lastResult == OrderTotalsProjected,
    lastResult == MissingPathSkipped,
  }
  val list_expansion_witness = lastResult == OrderTotalsProjected
  val projection_error_witness = or {
    lastResult == EmptyAttributePath,
    lastResult == InvalidAttributePath,
    lastResult == MissingExpressionAttributeName,
    lastResult == MissingExpressionAttributeNames,
  }
}
