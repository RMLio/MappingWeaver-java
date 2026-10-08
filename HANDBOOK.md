# MappingWeaver Handbook

## Preface: what MappingWeaver is (and what it is not)

MappingWeaver is an RML engine implemented in JAVA. Its job is to take an **input mapping file** (that itself references other data sources, files or through API) and output one or more **RDF output files** (or directly access writing APIs).

The RML engine mainly follows RML specifications set by the W3C KG-Construct Community Group, available at https://kg-construct.github.io/rml-resources/portal/, but it also supports the original RML.io specifications of https://rml.io/specs/rml/.

The mapping file is parsed through a rust dependency MappingLoom into a mapping plan, which is then executed.

This handbook is a tour of this MappingWeaver.

## Agent request contract (for AI agents/LLMs)

<!-- software-handbook contract: 2026-10-08 -->

Every implementation request handled by an AI agent/LLM follows these constraints:

- If the request is a feature or bugfix:
  - fix the specific failing case or issue named in the request;
  - preserve existing passing behavior unless explicitly asked not to;
  - add or update a regression test when needed.
- Make the smallest coherent patch. A documentation error found along the way is fixed in the same patch.
- Leave the code leaner after every request: remove what the change makes redundant (duplicate tests, parameters and options that no longer do anything, helpers that duplicate each other, comments that only repeat the code), and reuse shared functionality instead of adding a local variant. Run SpotBugs (`mvn compile spotbugs:check`, managed in `pom.xml`) and check the compiler warnings to find unused code.
- Fix a transient environment problem (a stale PATH, a shell or editor that needs a restart) in the environment, by restarting or reconfiguring it; add no code that works around it.
- **Push back** when a request would violate an established principle (e.g. breaking test hermeticity). Explain the principle and suggest a documentation-only fix instead of silently implementing the harmful change.
- Update this handbook so the change is documented as well as implemented.
  - Document only the latest state, integrated in the surrounding narrative (principles, behavior, rationale), including the choices made and why.
  - This contract holds only general rules for handling a request; project-specific guidance goes in the chapter on that topic.
- Do not stop at making tests green; align the implementation with the specification or intended design, and document the semantic reason in this handbook.
- Never remove or change existing tests (code or fixtures) without explicit permission. A change to an existing fixture (expected output, input, or data) is validated by the maintainer before it is kept, also when a tool writes it: propose the change with its reason, and keep it only after approval.
- Update `CHANGELOG.md` for every change, internal ones included (tests, CI, refactoring, removed code): keep `## Unreleased` a short summary of what changed since the last release. A feature that is new since the last release is one Added line, which later fixes update instead of getting lines of their own.
- Check whether `README.md` needs updates for user-visible behavior or workflow changes, and update it when needed.
- Write documentation (this handbook, READMEs, `TODO.md`, `CHANGELOG.md`, code comments) as plain positive statements: say what is true and leave out the contrast ("X, not Y"). Keep a negative only when it is the point itself, such as a prohibition, a warning, or a known limitation.
- If there are difficulties during fulfillment, document them in the most appropriate existing handbook location (create a new chapter only when truly necessary) so future requests start with better context.
- A preference or principle that the maintainer states while handling a request is documented so that every later request follows it: a general one in this contract (and in the software-handbook skill it comes from), a project-specific one in the handbook chapter it belongs to. When it is unclear which, ask.
- When a request is a list of feedback (such as a `TODO.md`), clean up after handling it: remove the items that are done, keep every open item as a clear task (an open question or an offered follow-up is an open item), and remove temporary files created along the way.

## Runtime logging

MappingWeaver logs through SLF4J, with `slf4j-simple` as the single runtime backend.

## FnO function descriptions

MappingWeaver bundles four built-in FnO description files (GREL and IDLab functions). They are referenced internally with a `classpath://` prefix so they are always loaded from the JAR's classpath and never accidentally shadowed by a same-named file in the working directory.

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
│   └── test-cases/                     # RML-KGC adaptations and regressions
│       ├── spec-adaptations/
│       ├── engines/
│       ├── integrations/
│       └── regressions/
├── mapping_plan/                      # Mapping-plan component fixtures
└── parsing/                           # Parser component fixtures
```

The Java test packages mirror this split: language-based tests live under `mappingweaver.rmlio.*` or `mappingweaver.rml_kgc.*`, while pure component tests live under `mappingweaver.components.*`. Shared test bases and extensions remain under `mappingweaver.cores` and `mappingweaver.utilities`; tests for package-private implementation classes remain beside those implementation packages.

Everything under each language's `spec/` directory is an immutable copy of an upstream specification suite. Tests may read these files but must never create, modify, rename, or delete files there.

## Continuous integration

`.gitlab-ci.yml` runs each test class as its own parallel job, in two matrices: `Specification Tests` (the spec suites) and `Utility Tests` (CLI, sources, FnO, regressions and components). Both extend the hidden `.unittests` job. A matrix lists only classes that run at least one test, so that every job checks something. A class whose tests are all disabled is added back once it is enabled. None of the listed classes use Testcontainers, so the jobs run without a Docker service; a job for an RDB or Kafka test needs that service again.

## Dependency versions

The versions of the KNoWS libraries MappingWeaver builds on are Maven properties in `pom.xml`: `amo.version` (algebraic-mapping-operators), `mappingloom.version` (the Java binding of algemaploom-rs), `function-agent.version`, `idlab-functions.version` and `grel-functions.version`. Each defaults to the latest release, so the committed build and CI resolve everything from Maven Central. A local build against development versions overrides them on the command line, e.g. `mvn test -Damo.version=5.0.1-SNAPSHOT`, after installing those versions locally. Upgrading a dependency means releasing it first and then setting its property to that release.
