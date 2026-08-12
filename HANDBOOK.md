# MappingWeaver Handbook

## Preface: what MappingWeaver is (and what it is not)

MappingWeaver is an RML engine implemented in JAVA. Its job is to take an **input mapping file** (that itself references other data sources, files or through API) and output one or more **RDF output files** (or directly access writing APIs).

The RML engine mainly follows RML specifications set by the W3C KG-Construct Community Group, available at https://kg-construct.github.io/rml-resources/portal/, but it also supports the original RML.io specifications of https://rml.io/specs/rml/.

The mapping file is parsed through a rust dependency MappingLoom into a mapping plan, which is then executed.

This handbook is a tour of this MappingWeaver.

## Agent request contract (for AI agents/LLMs)

Every implementation request handled by an AI agent/LLM should follow these constraints:

- If the request is a feature or bugfix:
  - fix the specific failing conformance case named in the request;
  - preserve existing passing behavior unless explicitly asked not to;
  - add or update a regression test when needed.
- Make the smallest coherent patch.
- **Push back** when a request would violate an established principle (e.g. breaking test hermeticity). Explain the principle and suggest a documentation-only fix instead of silently implementing the harmful change.
- Update this handbook so the request is documented, not only implemented.
  - Do not document all history of all requests (i.e., no changelog-style notes), instead, update documentation to reflect the latest state, integrated in the surrounding narrative (principles, behavior, rationale).
- Do not stop at making tests green; align implementation with the specification and document the semantic reason in this handbook.
- Never remove or change existing tests (code nor files) without explicit permission.
- Update `CHANGELOG.md` for implementation changes (update an existing unreleased entry or, if really relevant, add a new one); keep `## Unreleased` focused on the short diff from the previous version, not an accumulating list of every intermediate change.
- Check whether `README.md` needs updates for user-visible behavior or workflow changes, and update it when needed.
- If there are difficulties during fulfillment, document those difficulties in the most appropriate existing handbook location (create a new chapter only when truly necessary) so future requests start with better context.

## Runtime logging

MappingWeaver logs through SLF4J, with `slf4j-simple` as the runtime backend.

## FnO function descriptions

MappingWeaver bundles four built-in FnO description files (GREL and IDLab functions). They are referenced internally with a `classpath://` prefix so they are always loaded from the JAR's classpath and never accidentally shadowed by a same-named file in the working directory.

When `configure()` is called with custom descriptions, the effective set is built as follows:
- Each built-in whose filename (stripped of `classpath://`) matches a custom entry is evicted; the custom entry takes its place at the end of the list, ensuring it wins.
- If `customFunctionsOnly` is `true`, only the provided descriptions are used.

Custom descriptions without a `classpath://` prefix are resolved in this order:
1. Filesystem path (absolute, or relative to the JVM working directory).
2. Classpath fallback.

All descriptions are merged into a single Jena `Model` once (`effectiveModel`) and reused for both the parameter/return-type translators and the FnO `Agent` (which receives the model serialized back to Turtle to avoid re-reading the original files).
