# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Fixed
- A join on equality whose condition holds no attributes is refused instead of joining every combination. A mapping plan that cannot express its condition, one joining on a constant for instance, arrives with an empty condition; the loop over the attributes then found nothing to reject on and answered that every record matched, so a cross join was silently produced where an equality was asked for (RML-Core test cases RMLTC0030c to RMLTC0030f).
- An equality join no longer logs both solution mappings and both attribute names at WARN level for every pair of records it compares.

### Changed
- `bump-version.sh` moves the version to the next patch `-SNAPSHOT` after a pushed release, replacing the manual "Prepare for next development cycle" commit.
- Reorganized test resources and Java runners by RML language and provenance, using authoritative spec fixtures where applicable
- Updated dependency on Algebraic Mapping Operators to 5.0.0
- Updated dependency on MappingLoom to 0.8.0
- Updated dependency on idlab-functions-java to 1.5.1
- Updated dependencies on other libraries to latest stable release versions
- dataio is a direct dependency, since MappingWeaver uses its classes.
- The versions of algebraic-mapping-operators, dataio, MappingLoom, function-agent-java, idlab-functions-java and grel-functions-java are Maven properties, so a local build can use their development versions (see HANDBOOK: Dependency versions)
- Declared `slf4j-simple` as a runtime dependency, so the CLI has a concrete logging backend on its runtime classpath
- Built-in FnO descriptions use a `classpath://` prefix; custom descriptions can override them by filename (see README: Custom FnO function descriptions)
- GitLab CI: the test jobs extend a shared `.unittests` job, list only test classes that run at least one test (adding `ReferenceFunctionTest`), and run without a Docker service (see HANDBOOK: Continuous integration)
- A function producing several values hands them over as one `CollectionNode` instead of one node per value. The values stay together while they are passed around, so a function taking the result as an argument sees all of them, and the terms are generated where the collection is serialized: a `TemplateSerializer` states the template once per member. A collection that a term map asks to be serialized as an `rdf:List` or `rdf:Seq` is RML-CC and not implemented, so every collection is serialized a term at a time for now.

### Fixed
- `lookupWithDelimiter` calls using different input files no longer reuse each other's results
- Tests that change the FnO function descriptions restore the defaults afterwards, so the test classes that run after them in the same JVM find the built-in functions again
- An IRI or blank node object map fed by a function producing several values yields a term per value, instead of keeping only the first
- A function producing several values is applied to every one of them, wherever it is used
- FnO return datatypes now follow the selected resource in the function's ordered `fno:returns` list, with warnings and first-return fallback for invalid `rml:return` values
- A value from a multi-valued function no longer carries the datatype of the collection the function returns
- A source field is read once per name, so a logical view read by several triples maps no longer exhausts the heap
- A function returning its values as an array, as the GREL functions do, no longer ends up as the array itself
- A reference to something the record does not have yields NULL instead of aborting the mapping, as the [RML-IO registry](https://kg-construct.github.io/rml-io-registry/json-path/index.html#generation-of-null-values) requires of JSONPath
- An empty value is a value, and no longer reported as an absent attribute
- Git ignore `pom.xml.versionsBackup`
- Remove reference to non-existing branch in GitLab CI script
- Updated dependency on Function Agent to 1.5.1, which fixes a bug in calculating parameter arity for functions.
- Merged `--function-descriptions` and `-f` parameters.

### Removed
- The `--json-ld` option, which had no effect
- Unused classes `CSVSourceOperator`, `FlinkDataIOReader` (deprecated) and `Main.CommonSink`
- `CliOutputTest`, whose tests were all disabled
- Leftover copies of the `moveup` and `multiple-function-executions` mappings from before the test resources were reorganized

### Added
- Test cases `RMLFNOTC1005-JSON` and `RMLFNOTC1006-JSON`, covering a multi-valued function in an IRI and in a blank node object map
- Test cases `RMLFNOTC1001-JSON` to `RMLFNOTC1004-JSON`, covering a multi-valued function in a logical view: a split in an object map, with nulls turned into empty strings, with empty strings filtered out afterwards by `idlab-fn:trueCondition`, and the same split as a field of the view
- GitLab CI: added Javadoc check in linting phase
- Debug logging now reports the discovered/loaded FnO function IRIs when the FnO agent is initialized
- A parameter `--custom-functions-only` to disable loading of built-in FnO descriptions, so only the custom ones are used
- A parameter `--best-effort` to continue mapping even when encountering data errors.
- A test for loading and executing a RML mapping with a custom external function.
- A source that is a CSV on the Web table is read in the dialect it says it is written in. The plan's source configuration carries what the table and its dialect said — the delimiter, the quote character, the encoding, whether the data has a header row, and the values standing for no value — and those become the `CSVWConfiguration` the CSV source operator reads through. A source saying none of it is a plain CSV and is read as before. Test case RMLIOREGTC0012b covers a semicolon-separated table naming "NULL" as its null value.
- A file source may say where its data is with `url` instead of `path`, which is what a CSV on the Web table says.
- GitLab CI: create JAR artifact for each build.
## [0.3.0] - 2026-07-30

### Added
- Classified the RML-CC, RML-FNML and RML-STAR conformance test cases as passing or known-failing, each with an explanatory reason.
- RMLLVFNMLTest, covering a logical-view field whose value is computed by an FnO function (`toUpperCase`), and enabled it in the GitLab CI pipeline.

### Changed
- Updated Algebraic Mapping Operators to 4.0.0, MappingLoom to 0.7.1 and idlab-functions-java to 1.5.0.
- A source field's value is now read from MappingLoom's single `expression` instead of the separate `reference` and `constant` keys, which were fused as of MappingLoom 0.7.0. The expression is handed to an AMO `ExpressionField` as it is, whether it is a reference, a constant or a function computing the value; a function producing several values makes the field produce a record per value. This replaces the earlier mapping of a reference or constant onto AMO's now removed reference and constant fields.
- `ReferenceFunction` reports itself as a bare reference (`ExtendFunction.asReference()`), so a field carrying it reads the attribute straight from the record and a path matching several values (a JSON array, an XML node list) still yields all of them.
- RML-LV test case RMLLVTC0001c (a template-valued expression field) passes and is no longer listed as known-failing.
- Synced the RML-IO, RML-STAR and RML-FNML test resources with their upstream RML test-case repositories and re-triaged the affected tests.
- Enabled the passing spec test classes (RML-FNML, RML-STAR, RMLRegistry, WebSocket, FnO) in the GitLab CI pipeline.
- Updated the README conformance table with the current test-case pass percentages.

### Fixed
- A function that fails at runtime (e.g. a `substring` index out of range) now yields an empty result instead of aborting the whole mapping, so no triple is generated for that value; a function that cannot be resolved still raises an error (fixes RML-FNML test case RMLFNMLTC0008-CSV).
- Updated Algebraic Mapping Operators to 2.0.3
- Updated MappingLoom to 0.6.8
- Added GenerateBlankNode extend function, fixes RML-Core test case RMLTC0012e.

## [0.2.0] - 2026-05-27
### Added
- Upgraded Flink to v2.2.0 
- Implemented stdout sink using Flink 2.2's Sink API
- Added a FlinkTargetOperator to handle the creation of sinks using a Factory pattern
- Added an extra Flink operator step to extract the serialized RDF output from solution mappings before written the records into the sinks
- Added CLI option `-l --loom-file` to take an AlgeMapLoom plan as input.
- Added websocket support

### Fixed
- Updated RML test cases
- Put dependency to FnOio in small case. See https://github.com/RMLio/MappingWeaver-java/pull/1
- Update dependency on MappingLoom to 0.6.6
- Use base IRI given as program argument when generating relative IRIs

### Changed
- Updated Algemaploom-rs version to 0.6.5
- (Re-)use Flink 2.2's mini-cluster for testing and reduce test start-up time

### Refactored 
- Moved all CLI related parameters parsing and specification to a separate module
- Updated pom.xml to directly pull dataio instead of pulling this through algebraic mapping operators

### Removed
- Old TargetSinkFunction which implements the deprecated legacy Flink's SinkFunction<T> API
- OperatorTests.java which doesn't test anything meaningful is removed
- All existing implementations of a generic DataIO-based sink operators using legacy Flink's Source API

## [0.1.0] - 2025-10-08

### Added
- Initial source code

[0.3.0]: https://github.com/RMLio/MappingWeaver-java/releases/tag/v0.1.0
[0.2.0]: https://github.com/RMLio/MappingWeaver-java/releases/tag/v0.1.0
[0.1.0]: https://github.com/RMLio/MappingWeaver-java/releases/tag/v0.1.0
