module query {
  type QueryResult =
    | NotChecked
    | SelectAccepted
    | WithAccepted
    | MutatingSqlRejected
    | MultipleStatementsRejected
    | StructuredQueryCompiled
    | DocumentPathCompiled
    | UnregisteredColumnRejected
    | EmptySelectRejected

  type SqlPrefix = SelectPrefix | WithPrefix | InsertPrefix | UpdatePrefix | DeletePrefix | UnknownPrefix

  var lastResult: QueryResult
  var rawAccepted: bool
  var rawReadOnlyPrefix: bool
  var rawSingleStatement: bool
  var structuredAccepted: bool
  var structuredHasSelect: bool
  var selectedColumnsRegistered: bool
  var documentPathsAllowed: bool
  var filtersAllowed: bool
  var groupByAllowed: bool
  var orderByAllowed: bool
  var limitAllowed: bool
  var malformedPathUsed: bool

  pure val queryResults: Set[QueryResult] = Set(
    NotChecked,
    SelectAccepted,
    WithAccepted,
    MutatingSqlRejected,
    MultipleStatementsRejected,
    StructuredQueryCompiled,
    DocumentPathCompiled,
    UnregisteredColumnRejected,
    EmptySelectRejected
  )

  pure val sqlPrefixes: Set[SqlPrefix] = Set(
    SelectPrefix,
    WithPrefix,
    InsertPrefix,
    UpdatePrefix,
    DeletePrefix,
    UnknownPrefix
  )

  pure val statementCounts: Set[int] = Set(0, 1, 2)
  pure val selectCounts: Set[int] = Set(0, 1, 2)
  pure val bools: Set[bool] = Set(true, false)

  pure def isReadOnlyPrefix(prefix: SqlPrefix): bool =
    prefix == SelectPrefix or prefix == WithPrefix

  pure def isReadOnlyStatement(statement: str): bool =
    statement == "select" or statement == "with"

  pure def hasSingleStatement(statementCount: int): bool =
    statementCount == 1

  pure def rawAcceptedByGate(prefix: SqlPrefix, statementCount: int): bool =
    isReadOnlyPrefix(prefix) and hasSingleStatement(statementCount)

  pure def structuredShapeValid(selectCount: int, tableNamePresent: bool): bool =
    tableNamePresent and selectCount > 0

  pure def structuredColumnAllowed(columnRegistered: bool): bool =
    columnRegistered

  pure def documentPathAllowed(documentColumnRegistered: bool, usesConfiguredDocumentColumn: bool, pathValid: bool): bool =
    documentColumnRegistered and usesConfiguredDocumentColumn and pathValid

  pure def structuredAcceptedByGate(
    selectCount: int,
    selectedColumnRegistered: bool,
    usesDocumentPath: bool,
    documentColumnRegistered: bool,
    usesConfiguredDocumentColumn: bool,
    pathValid: bool,
    filterExpressionAllowed: bool,
    groupByExpressionAllowed: bool,
    orderByExpressionAllowed: bool,
    limitValid: bool,
  ): bool =
    structuredShapeValid(selectCount, true)
      and selectedColumnRegistered
      and (not(usesDocumentPath) or documentPathAllowed(documentColumnRegistered, usesConfiguredDocumentColumn, pathValid))
      and filterExpressionAllowed
      and groupByExpressionAllowed
      and orderByExpressionAllowed
      and limitValid

  pure def rawQueryResult(prefix: SqlPrefix, statementCount: int): QueryResult =
    if (rawAcceptedByGate(prefix, statementCount)) {
      if (prefix == SelectPrefix) SelectAccepted else WithAccepted
    } else if (not(hasSingleStatement(statementCount))) {
      MultipleStatementsRejected
    } else {
      MutatingSqlRejected
    }

  pure def structuredQueryResult(
    selectCount: int,
    selectedColumnRegistered: bool,
    usesDocumentPath: bool,
    documentColumnRegistered: bool,
    usesConfiguredDocumentColumn: bool,
    pathValid: bool,
    filterExpressionAllowed: bool,
    groupByExpressionAllowed: bool,
    orderByExpressionAllowed: bool,
    limitValid: bool,
  ): QueryResult =
    if (not(structuredShapeValid(selectCount, true))) {
      EmptySelectRejected
    } else if (not(selectedColumnRegistered)
        or (usesDocumentPath and not(documentPathAllowed(documentColumnRegistered, usesConfiguredDocumentColumn, pathValid)))
        or not(filterExpressionAllowed)
        or not(groupByExpressionAllowed)
        or not(orderByExpressionAllowed)
        or not(limitValid)) {
      UnregisteredColumnRejected
    } else if (usesDocumentPath) {
      DocumentPathCompiled
    } else {
      StructuredQueryCompiled
    }

  pure def queryCase(result: QueryResult): QueryResult =
    result

  action init = all {
    lastResult' = NotChecked,
    rawAccepted' = false,
    rawReadOnlyPrefix' = false,
    rawSingleStatement' = true,
    structuredAccepted' = false,
    structuredHasSelect' = true,
    selectedColumnsRegistered' = true,
    documentPathsAllowed' = true,
    filtersAllowed' = true,
    groupByAllowed' = true,
    orderByAllowed' = true,
    limitAllowed' = true,
    malformedPathUsed' = false,
  }

  action ResetBugHuntState = all {
    rawAccepted' = false,
    rawReadOnlyPrefix' = true,
    rawSingleStatement' = true,
    structuredAccepted' = false,
    structuredHasSelect' = true,
    selectedColumnsRegistered' = true,
    documentPathsAllowed' = true,
    filtersAllowed' = true,
    groupByAllowed' = true,
    orderByAllowed' = true,
    limitAllowed' = true,
    malformedPathUsed' = false,
  }

  action CheckSelectAccepted = all {
    lastResult' = queryCase(SelectAccepted),
    ResetBugHuntState,
  }

  action CheckWithAccepted = all {
    lastResult' = queryCase(WithAccepted),
    ResetBugHuntState,
  }

  action CheckMutatingSqlRejected = all {
    lastResult' = queryCase(MutatingSqlRejected),
    ResetBugHuntState,
  }

  action CheckMultipleStatementsRejected = all {
    lastResult' = queryCase(MultipleStatementsRejected),
    ResetBugHuntState,
  }

  action CheckStructuredQueryCompiled = all {
    lastResult' = queryCase(StructuredQueryCompiled),
    ResetBugHuntState,
  }

  action CheckDocumentPathCompiled = all {
    lastResult' = queryCase(DocumentPathCompiled),
    ResetBugHuntState,
  }

  action CheckUnregisteredColumnRejected = all {
    lastResult' = queryCase(UnregisteredColumnRejected),
    ResetBugHuntState,
  }

  action CheckEmptySelectRejected = all {
    lastResult' = queryCase(EmptySelectRejected),
    ResetBugHuntState,
  }

  action GenerateRawQueryCombination(prefix, statementCount) = all {
    rawAccepted' = rawAcceptedByGate(prefix, statementCount),
    rawReadOnlyPrefix' = isReadOnlyPrefix(prefix),
    rawSingleStatement' = hasSingleStatement(statementCount),
    structuredAccepted' = false,
    structuredHasSelect' = true,
    selectedColumnsRegistered' = true,
    documentPathsAllowed' = true,
    filtersAllowed' = true,
    groupByAllowed' = true,
    orderByAllowed' = true,
    limitAllowed' = true,
    malformedPathUsed' = false,
    lastResult' = rawQueryResult(prefix, statementCount),
  }

  action GenerateStructuredQueryCombination(
    selectCount,
    selectedColumnRegistered,
    usesDocumentPath,
    documentColumnRegistered,
    usesConfiguredDocumentColumn,
    pathValid,
    filterExpressionAllowed,
    groupByExpressionAllowed,
    orderByExpressionAllowed,
    limitValid,
  ) = all {
    rawAccepted' = false,
    rawReadOnlyPrefix' = true,
    rawSingleStatement' = true,
    structuredAccepted' = structuredAcceptedByGate(
      selectCount,
      selectedColumnRegistered,
      usesDocumentPath,
      documentColumnRegistered,
      usesConfiguredDocumentColumn,
      pathValid,
      filterExpressionAllowed,
      groupByExpressionAllowed,
      orderByExpressionAllowed,
      limitValid
    ),
    structuredHasSelect' = selectCount > 0,
    selectedColumnsRegistered' = selectedColumnRegistered,
    documentPathsAllowed' = not(usesDocumentPath) or documentPathAllowed(documentColumnRegistered, usesConfiguredDocumentColumn, pathValid),
    filtersAllowed' = filterExpressionAllowed,
    groupByAllowed' = groupByExpressionAllowed,
    orderByAllowed' = orderByExpressionAllowed,
    limitAllowed' = limitValid,
    malformedPathUsed' = usesDocumentPath and not(pathValid),
    lastResult' = structuredQueryResult(
      selectCount,
      selectedColumnRegistered,
      usesDocumentPath,
      documentColumnRegistered,
      usesConfiguredDocumentColumn,
      pathValid,
      filterExpressionAllowed,
      groupByExpressionAllowed,
      orderByExpressionAllowed,
      limitValid
    ),
  }

  action GenerateRawQuery = {
    nondet prefix = sqlPrefixes.oneOf()
    nondet statement_count = statementCounts.oneOf()
    GenerateRawQueryCombination(prefix, statement_count)
  }

  action GenerateStructuredQuery = {
    nondet select_count = selectCounts.oneOf()
    nondet selected_column_registered = bools.oneOf()
    nondet uses_document_path = bools.oneOf()
    nondet document_column_registered = bools.oneOf()
    nondet uses_configured_document_column = bools.oneOf()
    nondet path_valid = bools.oneOf()
    nondet filter_expression_allowed = bools.oneOf()
    nondet group_by_expression_allowed = bools.oneOf()
    nondet order_by_expression_allowed = bools.oneOf()
    nondet limit_valid = bools.oneOf()
    GenerateStructuredQueryCombination(
      select_count,
      selected_column_registered,
      uses_document_path,
      document_column_registered,
      uses_configured_document_column,
      path_valid,
      filter_expression_allowed,
      group_by_expression_allowed,
      order_by_expression_allowed,
      limit_valid
    )
  }

  action step = any {
    CheckSelectAccepted,
    CheckWithAccepted,
    CheckMutatingSqlRejected,
    CheckMultipleStatementsRejected,
    CheckStructuredQueryCompiled,
    CheckDocumentPathCompiled,
    CheckUnregisteredColumnRejected,
    CheckEmptySelectRejected,
    GenerateRawQuery,
    GenerateStructuredQuery,
  }

  /* Properties */

  val resultIsKnown = queryResults.contains(lastResult)
  val acceptedRawQueriesAreReadOnly =
    rawAccepted implies rawReadOnlyPrefix
  val acceptedRawQueriesAreSingleStatement =
    rawAccepted implies rawSingleStatement
  val acceptedStructuredQueriesHaveSelect =
    structuredAccepted implies structuredHasSelect
  val acceptedStructuredQueriesUseRegisteredColumns =
    structuredAccepted implies selectedColumnsRegistered
  val acceptedStructuredQueriesUseAllowedDocumentPaths =
    structuredAccepted implies documentPathsAllowed
  val acceptedStructuredQueriesHaveValidExpressions =
    structuredAccepted implies and {
      filtersAllowed,
      groupByAllowed,
      orderByAllowed,
    }
  val acceptedStructuredQueriesHaveValidLimits =
    structuredAccepted implies limitAllowed
  val acceptedStructuredQueriesNeverUseMalformedPaths =
    structuredAccepted implies not(malformedPathUsed)

  val inv = and {
    resultIsKnown,
    acceptedRawQueriesAreReadOnly,
    acceptedRawQueriesAreSingleStatement,
    acceptedStructuredQueriesHaveSelect,
    acceptedStructuredQueriesUseRegisteredColumns,
    acceptedStructuredQueriesUseAllowedDocumentPaths,
    acceptedStructuredQueriesHaveValidExpressions,
    acceptedStructuredQueriesHaveValidLimits,
    acceptedStructuredQueriesNeverUseMalformedPaths,
  }

  /* Example Runs */

  run rawSqlReadOnlyGateTest =
    all {
      assert(isReadOnlyStatement("select")),
      assert(isReadOnlyStatement("with")),
      assert(isReadOnlyStatement("insert") == false),
      assert(isReadOnlyPrefix(SelectPrefix)),
      assert(isReadOnlyPrefix(InsertPrefix) == false),
      assert(hasSingleStatement(1)),
      assert(hasSingleStatement(2) == false),
      assert(rawAcceptedByGate(SelectPrefix, 1)),
      assert(rawAcceptedByGate(DeletePrefix, 1) == false),
      assert(rawAcceptedByGate(SelectPrefix, 2) == false),
    }

  run structuredQueryShapeTest =
    all {
      assert(structuredShapeValid(1, true)),
      assert(structuredShapeValid(0, true) == false),
      assert(structuredShapeValid(1, false) == false),
    }

  run structuredQueryColumnAccessTest =
    all {
      assert(structuredColumnAllowed(true)),
      assert(structuredColumnAllowed(false) == false),
      assert(documentPathAllowed(true, true, true)),
      assert(documentPathAllowed(true, false, true) == false),
      assert(documentPathAllowed(true, true, false) == false),
      assert(structuredAcceptedByGate(1, true, true, true, true, true, true, true, true, true)),
      assert(structuredAcceptedByGate(1, true, true, true, true, false, true, true, true, true) == false),
    }

  /* Witnesses */

  val raw_query_success_witness = or {
    lastResult == SelectAccepted,
    lastResult == WithAccepted,
  }
  val raw_query_rejection_witness = or {
    lastResult == MutatingSqlRejected,
    lastResult == MultipleStatementsRejected,
  }
  val structured_query_witness = or {
    lastResult == StructuredQueryCompiled,
    lastResult == DocumentPathCompiled,
    lastResult == UnregisteredColumnRejected,
    lastResult == EmptySelectRejected,
  }
  val generated_raw_query_witness = rawAccepted
  val generated_structured_rejection_witness = and {
    not(structuredAccepted),
    or {
      not(structuredHasSelect),
      not(selectedColumnsRegistered),
      not(documentPathsAllowed),
      malformedPathUsed,
      not(filtersAllowed),
      not(groupByAllowed),
      not(orderByAllowed),
      not(limitAllowed),
    },
  }
}
