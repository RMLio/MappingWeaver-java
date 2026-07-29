# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- Classified the RML-CC, RML-FNML and RML-STAR conformance test cases as passing or known-failing, each with an explanatory reason.
- RMLLVFNMLTest, covering a logical-view field whose value is computed by an FnO function (`toUpperCase`), and enabled it in the GitLab CI pipeline.

### Changed
- Updated Algebraic Mapping Operators to 4.0.0-SNAPSHOT, MappingLoom to 0.7.1 and idlab-functions-java to 1.5.0.
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

[0.2.0]: https://github.com/RMLio/MappingWeaver-java/releases/tag/v0.1.0
[0.1.0]: https://github.com/RMLio/MappingWeaver-java/releases/tag/v0.1.0
