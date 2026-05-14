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

  var lastResult: ProjectionResult

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
    }

  action init =
    lastResult' = NotChecked

  action ProjectAliasPath =
    lastResult' = projectCase(AliasPathProjection)

  action ProjectListExpansion =
    lastResult' = projectCase(ListExpansionProjection)

  action ProjectMissingPath =
    lastResult' = projectCase(MissingPathProjection)

  action RejectEmptyAttributePath =
    lastResult' = projectCase(EmptyAttributePathProjection)

  action RejectInvalidAttributePath =
    lastResult' = projectCase(InvalidAttributePathProjection)

  action RejectAliasWithoutNameMap =
    lastResult' = projectCase(AliasWithoutNameMapProjection)

  action RejectMissingAlias =
    lastResult' = projectCase(MissingAliasProjection)

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

  val resultIsKnown = projectionResults.contains(lastResult)
  val inv = resultIsKnown

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
