# MappingWeaver Handbook

## Preface: what MappingWeaver is (and what it is not)

MappingWeaver is an RML engine implemented in JAVA. Its job is to take an **input mapping file** (that itself references other data sources, files or through API) and output one or more **RDF output files** (or directly access writing APIs).

The RML engine mainly follows RML specifications set by the W3C KG-Construct Community Group, available at https://kg-construct.github.io/rml-resources/portal/, but it also supports the original RML.io specifications of https://rml.io/specs/rml/.

The mapping file is parsed through a rust dependency MappingLoom into a mapping plan, which is then executed.

This handbook is a tour of this MappingWeaver.

## Agent request contract (for AI agents/LLMs)

<!-- software-handbook contract: 2026-10-13 -->

Every implementation request handled by an AI agent/LLM follows these constraints:

- If the request is a feature or bugfix:
  - fix the specific failing case or issue named in the request;
  - preserve existing passing behavior unless explicitly asked not to;
  - add or update a regression test when needed.
- Leave staging and committing to the maintainer. Edit staged files freely: the maintainer stages what they reviewed, so the unstaged diff shows the new changes.
- Make one coherent patch per request: everything the request needs (code, tests, docs, changelog) and nothing unrelated. A documentation error found along the way is fixed in the same patch.
- Leave the code leaner after every request: remove what the change makes redundant (duplicate tests, parameters and options that no longer do anything, helpers that duplicate each other, comments that only repeat the code), and reuse shared functionality instead of adding a local variant. Run SpotBugs (`mvn compile spotbugs:check`, managed in `pom.xml`) and check the compiler warnings to find unused code.
- Fix a transient environment problem (a stale PATH, a shell or editor that needs a restart) in the environment, by restarting or reconfiguring it; add no code that works around it.
- **Push back** when a request would violate an established principle (e.g. breaking test hermeticity). Explain the principle and suggest a documentation-only fix instead of silently implementing the harmful change.
- Update this handbook so the change is documented as well as implemented.
  - Document only the latest state, integrated in the surrounding narrative (principles, behavior, rationale), including the choices made and why.
  - This contract holds only general rules for handling a request; project-specific guidance goes in the chapter on that topic.
- Do not stop at making tests green; align the implementation with the specification or intended design, and document the semantic reason in this handbook.
- Update existing tests (code or fixtures) in the same patch when the change requires it, and list each update with its reason in the hand-over. Never weaken a test only to make it pass.
- Update `CHANGELOG.md` for every change, internal ones included (tests, CI, refactoring, removed code): keep `## Unreleased` a short summary of what changed since the last release. A feature that is new since the last release is one Added line, which later fixes update instead of getting lines of their own. Each line covers one concern; changes of the same concern share a line (such as editorial changes, or updated dependency versions). A line states what is true now, without the old behavior; a fix that only removes a divergence that should not have existed (one part behaving unlike the rest) gets a few words in the line of its concern at most.
- Before handing over, review the patch as the maintainer would (correctness, leaner code and docs, changelog) and fix what you find, so the maintainer's review is the last round.
- Before a release, review everything changed since the previous release (the code for correctness, the documentation and changelog for accuracy and brevity), check the dependencies (undeclared, outdated, vulnerable), and list the open items that change the released artifact, so they ship in this release.
- Hand over in one short message: what is ready (with a commit message), what needs the maintainer's judgement (each non-obvious removal or changed test, with its evidence), what was verified (commands and results), and the maintainer's next actions in order. Details belong in the changelog and docs.
- Check whether `README.md` needs updates for user-visible behavior or workflow changes, and update it when needed.
- Write documentation (this handbook, READMEs, `TODO.md`, `CHANGELOG.md`, code comments) as plain positive statements: say what is true and leave out the contrast ("X, not Y"). Keep a negative only when it is the point itself, such as a prohibition, a warning, or a known limitation.
- If there are difficulties during fulfillment, document them in the most appropriate existing handbook location (create a new chapter only when truly necessary) so future requests start with better context.
- A preference or principle that the maintainer states while handling a request is documented so that every later request follows it: a general one in this contract (and in the software-handbook skill it comes from), a project-specific one in the handbook chapter it belongs to. When it is unclear which, ask.
- When a request is a list of feedback (such as a `TODO.md`), clean up after handling it: remove the items that are done, keep every open item as a clear task (an open question or an offered follow-up is an open item), and remove temporary files created along the way.

## Command line

`Main.main` passes the exit code of `Main.run` to `System.exit`: 0 when the mapping ran (and for `--help`), 1 on invalid options and on every failure, whether the plan fails to translate or parse or the Flink job fails. Test runners and benchmarks (the kg-construct conformance runner, KROWN) judge a run by its exit code alone, so a failure that exits with 0 would count as a success. The explicit `System.exit` also ends the process when Flink threads linger after the job. Tests call `Main.run`, which returns the code without exiting the JVM (`CliExitCodeTest`).

Options are validated by picocli: exactly one of `-m` and `-l`, and `-o` for `toFile`. An invalid option prints the message and the usage to standard error.

`--version` prints `MappingWeaver <version>`, which the conformance runner records in its report. The version comes from the `Implementation-Version` that the shade plugin writes into the jar's manifest; run from the class folder (in tests) it is `unknown`.

`toFile -o <path>` writes through Flink's `FileSink`, so `<path>` is a directory of part files in a date-bucket subdirectory (`<path>/<yyyy-MM-dd--HH>/part-…`). For a bounded job the sink commits every part file at the end of the input, so no `.inprogress` file remains once the process has exited.

## Runtime logging

MappingWeaver logs through SLF4J, with `slf4j-simple` as the single runtime backend.

## FnO function descriptions

MappingWeaver has four built-in FnO description files (GREL and IDLab functions). None is a copy in this repository: `functions_grel.ttl` and `grel_java_mapping.ttl` come from the grel-functions-java jar, `fno/functions_idlab.ttl` and `fno/functions_idlab_classes_java_mapping.ttl` from the idlab-functions-java jar, so a dependency upgrade brings its descriptions along. They are referenced internally with a `classpath://` prefix so they are always loaded from the JAR's classpath and never accidentally shadowed by a same-named file in the working directory.

When `configure()` is called with custom descriptions, the effective set is built as follows:
- Each built-in whose filename (stripped of `classpath://`) matches a custom entry is evicted; the custom entry takes its place at the end of the list, ensuring it wins.
- If `customFunctionsOnly` is `true`, only the provided descriptions are used.

Custom descriptions without a `classpath://` prefix are resolved in this order:
1. Filesystem path (absolute, or relative to the JVM working directory).
2. Classpath fallback.

The effective descriptions are handed to `AgentFactory.createFromFnO` as they are. The resulting FnO `Agent` is created once per JVM, cached, and rebuilt after the next `configure()` call. The configuration is static and JVM-wide, so a test that calls `configure()` restores the default afterwards (`configure(List.of(), false)`); otherwise every test class that runs after it in the same JVM sees its descriptions.

For an FnO execution, `rml:return` is validated against that function's ordered `fno:returns` RDF list. A missing or invalid return resource falls back to the first list member; invalid or unverifiable declarations emit a warning, while a missing `rml:return` on a known function is logged at debug level.

## Test resource organization

Test resources are organized by provenance first and purpose second. Input format is only used below those boundaries. The intended structure is:

```text
src/test/resources/
├── rmlio/
│   ├── spec/                           # Immutable upstream RML-IO suites
│   └── test-cases/                     # RML-IO adaptations, regressions, and integrations
│       ├── spec-adaptations/
│       ├── engines/
│       ├── regressions/
│       └── integrations/
├── rml_kgc/
│   ├── spec/                           # Immutable upstream RML-KGC suites, including the RML registry suite
│   └── test-cases/                     # RML-KGC module tests, adaptations and regressions
│       ├── modules/
│       ├── spec-adaptations/
│       ├── engines/
│       ├── integrations/
│       └── regressions/
├── mapping_plan/                      # Mapping-plan component fixtures
└── parsing/                           # Parser component fixtures
```

The Java test packages mirror this split: language-based tests live under `mappingweaver.rmlio.*` or `mappingweaver.rml_kgc.*`, while pure component tests live under `mappingweaver.components.*`. Shared test bases and extensions remain under `mappingweaver.cores` and `mappingweaver.utilities`; tests for package-private implementation classes remain beside those implementation packages.

Everything under each language's `spec/` directory is an immutable copy of an upstream specification suite. Tests may read these files but must never create, modify, rename, or delete files there.

`rml_kgc/test-cases/modules/<module>/<group>/` holds the everyday tests of the RML-KGC modules, run by `ModuleTest`. Each group merges the kg-construct test cases of one feature that MappingWeaver passes in the conformance run into one mapping, so one Flink job checks many specification rules: each case's input files sit in a folder named after the case, an input file identical, apart from line endings, to one of an earlier case in the group is read from that case's folder, its triples maps and other named RML resources get the suffix `-<case id>`, and the expected `output.nq` is the union of the cases' expected outputs. The expected output then comes from upstream and needs no new approval. The union is right only when the merged cases cannot affect each other's output, so cases that generate blank nodes share a group only when they generate them from different values: blank nodes generated from equal values in one mapping are one node. `modules/README.md` lists the cases of each group and the suite commit they come from. `ModuleTest` compares the output as the conformance runner does, by dataset isomorphism (`TestCore.isomorphicTest`). It covers the cases with an expected output; the error cases are checked by the conformance run.

A case under `test-cases/regressions/` guards a fixed bug and always runs. Its expected output follows from the specification: the case names the rule it checks (in a README next to the fixtures, or in the test's comment), and its expected output is derived from that rule, never copied from what the engine produces, since a copied output locks the bug in. A new or changed expected output is approved by the maintainer, like any fixture change.

A regression that checks specification behaviour the upstream suite does not cover is also proposed upstream (to the kg-construct test cases). It stays in `regressions/` until upstream accepts it; then the copy under `spec/` takes over and the local case is removed.

## Conformance

MappingWeaver's conformance to RML-KGC is measured on the seven kg-construct test suites (rml-core, rml-io, rml-io-registry, rml-fnml, rml-cc, rml-lv and rml-star) with [mapping-test-runner](https://gitlab.ilabt.imec.be/rml/util/mapping-test-runner). The runner calls the jar's command line once per case (the engine file `conformance/mappingweaver.yaml`) and compares the output with the expected output as RDF datasets: graphs matched by name, one blank node mapping across all graphs, literals compared as terms. It compares every logical target, and an error case passes when the process exits non-zero, whatever it wrote. A runner release pins each suite at a commit, so the runner version is the version of the suites.

`conformance/run.sh` builds the jar, downloads the runner release it names (`MTR_VERSION`), runs the suites with `--jobs 4` and writes `target/conformance/report.json`, `report.md` (the format of the plan's coverage document) and `junit.xml`. Its arguments go to the Maven build, e.g. `-Dmappingloom.version=0.8.1-SNAPSHOT` for a locally installed translator; `MTR_JAR` uses a local runner jar instead of the download. The database cases run in Docker, and are skipped without it.

`conformance/expected-results.csv` lists every case that MappingWeaver does not pass, with a note on the cause that names the step, issue or spec adaptation that addresses it; an unlisted case is expected to pass. A run fails on any difference from the file: a case that does worse is a regression, and a case that does better is an unexpected pass. A change that makes a case pass therefore removes its line, and the file's history is the history of MappingWeaver's coverage.

The manual `Conformance` CI job runs the script on Docker-in-Docker and keeps the reports and each case's engine logs. It runs before every release ([RELEASE.md](RELEASE.md)) and for a change that targets conformance; everyday work relies on `ModuleTest` (Test resource organization). algemaploom-rs runs the same script with its local translator, in its own manual job, with unexpected passes as warnings, since it cannot edit this file.

The runner judges as the specification does, which the spec test classes under `rml_kgc/spec` did not: it reads the upstream cases at the pinned commits (the copies under `spec/` predate upstream changes, such as JSON integers typed `xsd:integer`), compares by isomorphism, compares the logical targets, and counts an empty output as an output.

## Generating IRIs

`IriTypeFunction` follows RML-Core: a value that is an absolute IRI is the IRI, any other value gets the base IRI prepended, and a value that still gives no valid IRI generates no term (logged as a warning). An absolute IRI is one with a scheme, of any scheme (`urn:`, `mailto:`, …), and optionally a fragment, as in RDF; Jena's `IRIx.isReference()` checks it. A constant is used as it is. A URL validator is the wrong tool here: it accepts only some schemes, and its rules for percent-encoded paths change between versions.

## Continuous integration

`.gitlab-ci.yml` runs each test class as its own parallel job, in two matrices: `Module Tests` (`ModuleTest` and `RMLLVFNMLTest`) and `Utility Tests` (CLI, sources, FnO, regressions and components). Both extend the hidden `.unittests` job. A matrix lists only classes that run at least one test, so that every job checks something. A class whose tests are all disabled is added back once it is enabled. The spec test classes that read the copies under `rml_kgc/spec/` are left out: `ModuleTest` and the conformance run check what they checked. None of the listed classes use Testcontainers, so these jobs run without a Docker service; a job for an RDB or Kafka test extends the hidden `.docker` job (Docker-in-Docker), as the `Conformance` job does.

A known limitation: in one JVM (`mvn test`), the jobs that `CliExitCodeTest` runs in-process through `Main.run` start a Flink cluster of their own next to the shared mini-cluster, and while a concurrent class (`ModuleTest`, or a spec test class) runs its jobs, a TaskManager heartbeat can time out and fail either class. Each class passes in a JVM of its own, which is how CI runs them; locally, run `CliExitCodeTest` apart (`mvn test -Dtest=CliExitCodeTest`).

## Dependency versions

The versions of the KNoWS libraries MappingWeaver builds on are Maven properties in `pom.xml`: `amo.version` (algebraic-mapping-operators), `dataio.version` (dataio, whose access, compression and CSVW classes the plan parser uses directly), `mappingloom.version` (the Java binding of algemaploom-rs), `function-agent.version`, `idlab-functions.version` and `grel-functions.version`. Each defaults to the latest release, so the committed build and CI resolve everything from Maven Central. A local build against development versions overrides them on the command line, e.g. `mvn test -Damo.version=5.0.1-SNAPSHOT`, after installing those versions locally. Upgrading a dependency means releasing it first and then setting its property to that release.

## Release process

Step-by-step instructions are in [RELEASE.md](RELEASE.md); this section explains the tooling.

`./bump-version.sh <version>` sets the version in `pom.xml` (`mvn versions:set`) and in the jar name in `README.md`, optionally adds the version section to `CHANGELOG.md` with `changefrog`, and optionally commits, tags (`v<version>`, or the bare name for `testrelease-*`) and pushes; after a pushed release other than a `testrelease-*`, it moves the version to the next patch `-SNAPSHOT` (e.g. `0.4.1-SNAPSHOT` after `0.4.0`) and commits and pushes that as "Prepare for next development cycle".
