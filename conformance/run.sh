#!/usr/bin/env bash
# Runs the kg-construct conformance suites on MappingWeaver with mapping-test-runner (HANDBOOK: Conformance).
# Arguments are passed to the Maven build of the jar, e.g. -Dmappingloom.version=0.8.1-SNAPSHOT.
# Environment:
#   MTR_VERSION          the runner release to download (default below)
#   MTR_JAR              a runner jar to use instead of the download
#   MTR_UNEXPECTED_PASS  fail (default) or warn
#   MTR_JOBS             the number of cases that run at the same time (default 4)
#   MTR_CACHE            where the runner keeps the fetched suites (its default: ~/.cache/mapping-test-runner)
# Writes target/conformance/report.{json,md} and junit.xml; exits with the status of `mtr run`.
set -euo pipefail

MTR_VERSION="${MTR_VERSION:-0.1.2}"
cd "$(dirname "$0")/.."
out=target/conformance
mkdir -p "$out"
rm -f "$out/report.json" "$out/report.md" "$out/junit.xml"

mvn --batch-mode --quiet -DskipTests package "$@"
jar=$(ls -t target/MappingWeaver-*-shaded.jar | head -n 1)

if [ -z "${MTR_JAR:-}" ]; then
    MTR_JAR="$out/mapping-test-runner-$MTR_VERSION.jar"
    if [ ! -f "$MTR_JAR" ]; then
        curl -fsSL -o "$MTR_JAR.part" \
            "https://repo1.maven.org/maven2/be/ugent/idlab/knows/mapping-test-runner/$MTR_VERSION/mapping-test-runner-$MTR_VERSION.jar"
        mv "$MTR_JAR.part" "$MTR_JAR"
    fi
fi

status=0
java -jar "$MTR_JAR" run --engine conformance/mappingweaver.yaml --set "jar=$jar" \
    --corpus rml-core,rml-io,rml-io-registry,rml-fnml,rml-cc,rml-lv,rml-star \
    --expectations conformance/expected-results.csv --unexpected-pass "${MTR_UNEXPECTED_PASS:-fail}" \
    --jobs "${MTR_JOBS:-4}" --out "$out/report.json" --run-dir "$out/mtr-run" || status=$?
if [ -f "$out/report.json" ]; then
    java -jar "$MTR_JAR" report "$out/report.json" --md > "$out/report.md"
    java -jar "$MTR_JAR" report "$out/report.json" --junit > "$out/junit.xml"
fi
exit "$status"
