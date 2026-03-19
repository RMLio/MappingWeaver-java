# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
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

[0.1.0]: https://github.com/RMLio/MappingWeaver-java/releases/tag/v0.1.0