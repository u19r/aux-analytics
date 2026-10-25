// Configuration is resolved once at the caller boundary. CLI and Lambda use
// an explicit manifest argument/environment value before analytics.manifest_path;
// the API may intentionally start with an empty manifest, while malformed or
// omitted values still fail during parsing. Source and retention semantics are
// validated after precedence is resolved, so no caller can silently select a
// different policy.
module config_api {
  type Caller = Cli | Api | Lambda
  type StreamKind = NoStream | StorageStream | AuxStorage
  type ManifestSource = ExplicitArgument | ConfigFile | ApiEmpty | Missing
  type ConfigResult =
    | NotChecked
    | Accepted
    | AcceptedEmptyApi
    | RejectedMissingManifest
    | RejectedMalformedManifest
    | RejectedSourceStreamType
    | RejectedRetentionBounds
    | RejectedQueryLimit

  type ConfigInput = {
    caller: Caller,
    manifestArgumentPresent: bool,
    manifestArgumentNonEmpty: bool,
    manifestConfigPresent: bool,
    manifestConfigNonEmpty: bool,
    sourceTableConfigured: bool,
    sourceGlobalStream: StreamKind,
    sourceTableStream: StreamKind,
    retentionEnabled: bool,
    retentionQueryConfigured: bool,
    retentionSweepInterval: int,
    retentionDeleteBatch: int,
    queryLimitPresent: bool,
    queryLimit: int,
    payloadWellFormed: bool,
    ingestEndpointEnabled: bool,
    ingestProcessorEnabled: bool,
  }

  type ConfigState = {
    lastResult: ConfigResult,
    manifestSource: ManifestSource,
    effectiveStream: StreamKind,
    ingestRouteEnabled: bool,
  }

  pure val callers: Set[Caller] = Set(Cli, Api, Lambda)
  pure val streams: Set[StreamKind] = Set(NoStream, StorageStream, AuxStorage)
  pure val bools: Set[bool] = Set(true, false)
  pure val limits: Set[int] = Set(0, 1, 2, 10_000)

  pure def defaultInput: ConfigInput = {
    caller: Api,
    manifestArgumentPresent: false,
    manifestArgumentNonEmpty: false,
    manifestConfigPresent: false,
    manifestConfigNonEmpty: false,
    sourceTableConfigured: false,
    sourceGlobalStream: NoStream,
    sourceTableStream: NoStream,
    retentionEnabled: false,
    retentionQueryConfigured: false,
    retentionSweepInterval: 1,
    retentionDeleteBatch: 1,
    queryLimitPresent: false,
    queryLimit: 1,
    payloadWellFormed: true,
    ingestEndpointEnabled: true,
    ingestProcessorEnabled: true,
  }

  pure def manifestSource(input: ConfigInput): ManifestSource =
    if (input.manifestArgumentPresent) ExplicitArgument
    else if (input.manifestConfigPresent) ConfigFile
    else if (input.caller == Api) ApiEmpty
    else Missing

  pure def effectiveStream(input: ConfigInput): StreamKind =
    if (input.sourceTableConfigured and input.sourceTableStream != NoStream)
      input.sourceTableStream
    else input.sourceGlobalStream

  pure def malformedManifest(input: ConfigInput): bool =
    if (input.manifestArgumentPresent)
      not(input.manifestArgumentNonEmpty) or not(input.payloadWellFormed)
    else if (input.manifestConfigPresent)
      not(input.manifestConfigNonEmpty) or not(input.payloadWellFormed)
    else false

  pure def manifestMissing(input: ConfigInput): bool =
    not(input.manifestArgumentPresent) and not(input.manifestConfigPresent)

  pure def sourceConfigValid(input: ConfigInput): bool =
    not(input.sourceTableConfigured) or effectiveStream(input) != NoStream

  pure def retentionConfigValid(input: ConfigInput): bool =
    not(input.retentionEnabled) or (
      input.retentionSweepInterval > 0 and
      input.retentionDeleteBatch > 0
    )

  pure def queryConfigValid(input: ConfigInput): bool =
    not(input.retentionEnabled and input.retentionQueryConfigured) or
      (input.queryLimitPresent and input.queryLimit == 1)

  pure def expectedResult(input: ConfigInput): ConfigResult =
    if (not(input.queryLimitPresent) and input.retentionEnabled and input.retentionQueryConfigured)
      RejectedQueryLimit
    else if (malformedManifest(input)) RejectedMalformedManifest
    else if (manifestMissing(input) and input.caller != Api) RejectedMissingManifest
    else if (not(sourceConfigValid(input))) RejectedSourceStreamType
    else if (not(retentionConfigValid(input))) RejectedRetentionBounds
    else if (not(queryConfigValid(input))) RejectedRetentionBounds
    else if (manifestMissing(input)) AcceptedEmptyApi
    else Accepted

  pure def routeEnabled(input: ConfigInput, result: ConfigResult): bool =
    (result == Accepted or result == AcceptedEmptyApi) and
      input.ingestEndpointEnabled and input.ingestProcessorEnabled

  pure def stateFor(input: ConfigInput): ConfigState = {
    lastResult: expectedResult(input),
    manifestSource: if (expectedResult(input) == Accepted or expectedResult(input) == AcceptedEmptyApi) manifestSource(input) else Missing,
    effectiveStream: if (expectedResult(input) == Accepted or expectedResult(input) == AcceptedEmptyApi) effectiveStream(input) else NoStream,
    ingestRouteEnabled: routeEnabled(input, expectedResult(input)),
  }

  var state: ConfigState

  action init = state' = {
    lastResult: NotChecked,
    manifestSource: Missing,
    effectiveStream: NoStream,
    ingestRouteEnabled: false,
  }

  action CheckConfig = {
    nondet caller = callers.oneOf()
    nondet argumentPresent = bools.oneOf()
    nondet argumentNonEmpty = bools.oneOf()
    nondet configPresent = bools.oneOf()
    nondet configNonEmpty = bools.oneOf()
    nondet tableConfigured = bools.oneOf()
    nondet globalStream = streams.oneOf()
    nondet tableStream = streams.oneOf()
    nondet retentionEnabled = bools.oneOf()
    nondet queryConfigured = bools.oneOf()
    nondet sweepInterval = limits.oneOf()
    nondet deleteBatch = limits.oneOf()
    nondet queryLimitPresent = bools.oneOf()
    nondet queryLimit = limits.oneOf()
    nondet payloadWellFormed = bools.oneOf()
    nondet endpointEnabled = bools.oneOf()
    nondet processorEnabled = bools.oneOf()
    val input: ConfigInput = {
      caller: caller,
      manifestArgumentPresent: argumentPresent,
      manifestArgumentNonEmpty: argumentNonEmpty,
      manifestConfigPresent: configPresent,
      manifestConfigNonEmpty: configNonEmpty,
      sourceTableConfigured: tableConfigured,
      sourceGlobalStream: globalStream,
      sourceTableStream: tableStream,
      retentionEnabled: retentionEnabled,
      retentionQueryConfigured: queryConfigured,
      retentionSweepInterval: sweepInterval,
      retentionDeleteBatch: deleteBatch,
      queryLimitPresent: queryLimitPresent,
      queryLimit: queryLimit,
      payloadWellFormed: payloadWellFormed,
      ingestEndpointEnabled: endpointEnabled,
      ingestProcessorEnabled: processorEnabled,
    }
    all { state' = stateFor(input) }
  }

  action step = CheckConfig

  val acceptedConfigHasResolvedManifest =
    if (state.lastResult == Accepted or state.lastResult == AcceptedEmptyApi)
      state.manifestSource != Missing
    else true
  val rejectedConfigHasNoResolvedOutput =
    if (state.lastResult == Accepted or state.lastResult == AcceptedEmptyApi)
      true
    else state.manifestSource == Missing and state.effectiveStream == NoStream
  val rejectedConfigDoesNotEnableRoute =
    if (state.lastResult == Accepted or state.lastResult == AcceptedEmptyApi)
      true
    else not(state.ingestRouteEnabled)
  val cli_missing_manifest_witness =
    state.lastResult == RejectedMissingManifest
  val api_empty_manifest_witness =
    state.lastResult == AcceptedEmptyApi
  val lambda_argument_precedence_witness =
    state.lastResult == Accepted and state.manifestSource == ExplicitArgument
  val malformed_manifest_witness = state.lastResult == RejectedMalformedManifest
  val invalid_source_witness = state.lastResult == RejectedSourceStreamType
  val invalid_retention_witness = state.lastResult == RejectedRetentionBounds
  val invalid_query_limit_witness = state.lastResult == RejectedRetentionBounds
  val max_query_limit_witness =
    state.lastResult == Accepted
  val omitted_query_limit_witness =
    state.lastResult == RejectedQueryLimit
  val explicit_query_limit_witness =
    state.lastResult == Accepted

  val inv = all {
    acceptedConfigHasResolvedManifest,
    rejectedConfigHasNoResolvedOutput,
    rejectedConfigDoesNotEnableRoute,
  }

  run cliMissingManifestWitness = assert(
    expectedResult({
      caller: Cli,
      manifestArgumentPresent: false,
      manifestArgumentNonEmpty: false,
      manifestConfigPresent: false,
      manifestConfigNonEmpty: false,
      sourceTableConfigured: false,
      sourceGlobalStream: NoStream,
      sourceTableStream: NoStream,
      retentionEnabled: false,
      retentionQueryConfigured: false,
      retentionSweepInterval: 1,
      retentionDeleteBatch: 1,
      queryLimitPresent: false,
      queryLimit: 1,
      payloadWellFormed: true,
      ingestEndpointEnabled: true,
      ingestProcessorEnabled: true,
    }) == RejectedMissingManifest
  )
  run apiEmptyManifestWitness = assert(expectedResult(defaultInput) == AcceptedEmptyApi)
  run apiEmptyIgnoresUnreferencedMalformedPayloadWitness = assert(
    expectedResult({
      caller: Api,
      manifestArgumentPresent: false,
      manifestArgumentNonEmpty: false,
      manifestConfigPresent: false,
      manifestConfigNonEmpty: false,
      sourceTableConfigured: false,
      sourceGlobalStream: NoStream,
      sourceTableStream: NoStream,
      retentionEnabled: false,
      retentionQueryConfigured: false,
      retentionSweepInterval: 1,
      retentionDeleteBatch: 1,
      queryLimitPresent: false,
      queryLimit: 1,
      payloadWellFormed: false,
      ingestEndpointEnabled: true,
      ingestProcessorEnabled: true,
    }) == AcceptedEmptyApi
  )
  run lambdaArgumentPrecedenceWitness = assert(
    manifestSource({
      caller: Lambda,
      manifestArgumentPresent: true,
      manifestArgumentNonEmpty: true,
      manifestConfigPresent: true,
      manifestConfigNonEmpty: true,
      sourceTableConfigured: false,
      sourceGlobalStream: NoStream,
      sourceTableStream: NoStream,
      retentionEnabled: false,
      retentionQueryConfigured: false,
      retentionSweepInterval: 1,
      retentionDeleteBatch: 1,
      queryLimitPresent: true,
      queryLimit: 1,
      payloadWellFormed: true,
      ingestEndpointEnabled: true,
      ingestProcessorEnabled: true,
    }) == ExplicitArgument
  )
  run malformedManifestWitness = assert(
    expectedResult({
      caller: Cli,
      manifestArgumentPresent: true,
      manifestArgumentNonEmpty: false,
      manifestConfigPresent: true,
      manifestConfigNonEmpty: true,
      sourceTableConfigured: false,
      sourceGlobalStream: NoStream,
      sourceTableStream: NoStream,
      retentionEnabled: false,
      retentionQueryConfigured: false,
      retentionSweepInterval: 1,
      retentionDeleteBatch: 1,
      queryLimitPresent: true,
      queryLimit: 1,
      payloadWellFormed: true,
      ingestEndpointEnabled: true,
      ingestProcessorEnabled: true,
    }) == RejectedMalformedManifest
  )
  run invalidSourceWitness = assert(
    expectedResult({
      caller: Cli,
      manifestArgumentPresent: true,
      manifestArgumentNonEmpty: true,
      manifestConfigPresent: false,
      manifestConfigNonEmpty: false,
      sourceTableConfigured: true,
      sourceGlobalStream: NoStream,
      sourceTableStream: NoStream,
      retentionEnabled: false,
      retentionQueryConfigured: false,
      retentionSweepInterval: 1,
      retentionDeleteBatch: 1,
      queryLimitPresent: true,
      queryLimit: 1,
      payloadWellFormed: true,
      ingestEndpointEnabled: true,
      ingestProcessorEnabled: true,
    }) == RejectedSourceStreamType
  )
  run invalidRetentionWitness = assert(
    expectedResult({
      caller: Cli,
      manifestArgumentPresent: true,
      manifestArgumentNonEmpty: true,
      manifestConfigPresent: false,
      manifestConfigNonEmpty: false,
      sourceTableConfigured: false,
      sourceGlobalStream: NoStream,
      sourceTableStream: NoStream,
      retentionEnabled: true,
      retentionQueryConfigured: false,
      retentionSweepInterval: 0,
      retentionDeleteBatch: 1,
      queryLimitPresent: true,
      queryLimit: 1,
      payloadWellFormed: true,
      ingestEndpointEnabled: true,
      ingestProcessorEnabled: true,
    }) == RejectedRetentionBounds
  )
  run invalidQueryLimitWitness = assert(
    expectedResult({
      caller: Cli,
      manifestArgumentPresent: true,
      manifestArgumentNonEmpty: true,
      manifestConfigPresent: false,
      manifestConfigNonEmpty: false,
      sourceTableConfigured: false,
      sourceGlobalStream: NoStream,
      sourceTableStream: NoStream,
      retentionEnabled: true,
      retentionQueryConfigured: true,
      retentionSweepInterval: 1,
      retentionDeleteBatch: 1,
      queryLimitPresent: true,
      queryLimit: 10_000,
      payloadWellFormed: true,
      ingestEndpointEnabled: true,
      ingestProcessorEnabled: true,
    }) == RejectedRetentionBounds
  )

  run maxQueryLimitWitness = assert(
    expectedResult({
      caller: Cli,
      manifestArgumentPresent: true,
      manifestArgumentNonEmpty: true,
      manifestConfigPresent: false,
      manifestConfigNonEmpty: false,
      sourceTableConfigured: false,
      sourceGlobalStream: NoStream,
      sourceTableStream: NoStream,
      retentionEnabled: true,
      retentionQueryConfigured: true,
      retentionSweepInterval: 1,
      retentionDeleteBatch: 1,
      queryLimitPresent: true,
      queryLimit: 1,
      payloadWellFormed: true,
      ingestEndpointEnabled: true,
      ingestProcessorEnabled: true,
    }) == Accepted
  )

  run omittedQueryLimitWitness = assert(
    expectedResult({
      caller: Cli,
      manifestArgumentPresent: true,
      manifestArgumentNonEmpty: true,
      manifestConfigPresent: false,
      manifestConfigNonEmpty: false,
      sourceTableConfigured: false,
      sourceGlobalStream: NoStream,
      sourceTableStream: NoStream,
      retentionEnabled: true,
      retentionQueryConfigured: true,
      retentionSweepInterval: 1,
      retentionDeleteBatch: 1,
      queryLimitPresent: false,
      queryLimit: 1,
      payloadWellFormed: true,
      ingestEndpointEnabled: true,
      ingestProcessorEnabled: true,
    }) == RejectedQueryLimit
  )

  run explicitQueryLimitWitness = assert(
    expectedResult({
      caller: Cli,
      manifestArgumentPresent: true,
      manifestArgumentNonEmpty: true,
      manifestConfigPresent: false,
      manifestConfigNonEmpty: false,
      sourceTableConfigured: false,
      sourceGlobalStream: NoStream,
      sourceTableStream: NoStream,
      retentionEnabled: true,
      retentionQueryConfigured: true,
      retentionSweepInterval: 1,
      retentionDeleteBatch: 1,
      queryLimitPresent: true,
      queryLimit: 1,
      payloadWellFormed: true,
      ingestEndpointEnabled: true,
      ingestProcessorEnabled: true,
    }) == Accepted
  )
}
