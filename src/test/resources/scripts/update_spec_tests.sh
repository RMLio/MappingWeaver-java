#!/bin/bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/../../../.." && pwd)"
remote_root="$repo_root/remote_tests"

cleanup() {
  rm -rf "$remote_root"
}
trap cleanup EXIT

mkdir -p "$remote_root"

copy_cases() {
  local checkout_dir="$1"
  local destination="$2"

  mkdir -p "$repo_root/$destination"
  find "$checkout_dir" -type d -name 'RML*' -print0 |
    while IFS= read -r -d '' case_dir; do
      cp -a "$case_dir" "$repo_root/$destination/"
    done
}

pull_cases() {
  local name="$1"
  local repository="$2"
  local destination="$3"
  local sparse_path="${4:-test-cases}"
  local checkout_dir="$remote_root/$name"

  git clone --filter=blob:none --sparse "$repository" "$checkout_dir"
  (
    cd "$checkout_dir"
    git sparse-checkout set "$sparse_path"
  )
  copy_cases "$checkout_dir" "$destination"
}

# RML-KGC specification modules.
declare -a kgc_modules=(
  "rml-core|https://github.com/kg-construct/rml-core|src/test/resources/rml_kgc/spec/rml-core"
  "rml-io|https://github.com/kg-construct/rml-io|src/test/resources/rml_kgc/spec/rml-io"
  "rml-cc|https://github.com/kg-construct/rml-cc|src/test/resources/rml_kgc/spec/rml-cc"
  "rml-fnml|https://github.com/kg-construct/rml-fnml|src/test/resources/rml_kgc/spec/rml-fnml"
  "rml-star|https://github.com/kg-construct/rml-star|src/test/resources/rml_kgc/spec/rml-star"
  "rml-lv|https://github.com/kg-construct/rml-lv|src/test/resources/rml_kgc/spec/rml-lv"
  "rml-io-registry|https://github.com/kg-construct/rml-io-registry|src/test/resources/rml_kgc/spec/rml-io-registry"
)

for module in "${kgc_modules[@]}"; do
  IFS='|' read -r name repository destination <<< "$module"
  pull_cases "$name" "$repository" "$destination"
done

# RML-IO specification modules.
pull_cases "rmlio-core" "https://github.com/kg-construct/rml-test-cases" "src/test/resources/rmlio/spec/core"
pull_cases "rmlio-fno" "https://github.com/RMLio/rml-fno-test-cases" "src/test/resources/rmlio/spec/fno" "."

echo "Updated specification fixtures. Review the diff and commit manually if appropriate."