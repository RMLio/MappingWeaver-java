#!/bin/bash

# ===================================================================== #
# Bash script to compare remote-tests with local tests in project.
# Doesn't change current Git repository. Puts output in markdown file.
#
# High-level check: files/directories added/removed?
# Low-level check: files changed (content)?
#
# Steps:
# 1. Download remote test-cases from existing module repositories.
# 2. Place them in a correct directory structure.
# 3. Compare remote with local version (High-level + Low-level).
# 4. Clean up remote the remote test-cases.
#
# Author: Stijn Van Biesen
# ===================================================================== #

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/../../../.." && pwd)"
cd "$repo_root" || exit 1

#Create log-file.
log_file="src/test/resources/scripts/diff_log.md"
> "$log_file"

# Compare only files that are not ignored by this repository.
exclude_file=$(mktemp)
git ls-files --others --ignored --exclude-standard -- \
  src/test/resources/rmlio/spec src/test/resources/rml_kgc/spec |
  while IFS= read -r path; do basename "$path"; done |
  sort -u > "$exclude_file"

file_list() {
  (cd "$1" && find . -type f | grep -v -F -f "$exclude_file" | sort)
}

report_high_level_differences() {
  local base1="$1"
  local base2="$2"
  local adaptation_dir="$3"

  local local_only
  local remote_only
  local_only=$(comm -23 <(file_list "$base1") <(file_list "$base2"))
  remote_only=$(comm -13 <(file_list "$base1") <(file_list "$base2"))

  if [ -n "$local_only" ]; then
    {
      echo "**Local-only tests detected: move these to $adaptation_dir:**"
      echo '```text'
      echo "$local_only"
      echo '```'
    } >> "./$log_file"
  fi

  if [ -n "$remote_only" ]; then
    {
      echo "**Remote-only tests detected:**"
      echo '```text'
      echo "$remote_only"
      echo '```'
    } >> "./$log_file"
  fi
}

# rml_kgc

#Create temporary directory structure
mkdir -p remote_tests/resources/rml_kgc/spec/
cd remote_tests
declare -A repositories
declare -A commits

modules=(rml-core rml-io rml-cc rml-fnml rml-star rml-lv rml-io-registry)

for name in "${modules[@]}"; do

  #Cloning test-cases folders from different repositories.
  mkdir -p resources/rml_kgc/spec/$name
  repo="https://github.com/kg-construct/$name"

  if [[ -d $name ]]; then #Should not happen...
    cd $name
    git pull
    cd -
  else
    git clone --filter=blob:none --sparse $repo $name
    cd $name
    git sparse-checkout set test-cases
    commit=$(git rev-parse HEAD)
    repositories["$name"]="${repo}"
    commits["$name"]="$(date +%F\ %T): ${repo}/commit/${commit}"
    cd -
  fi

  #Moving all test cases according given directory structure.
  find $name -type d | grep "RML" | xargs -I '{}' cp -r {} resources/rml_kgc/spec/$name/
done

# Step out of remote_tests.
cd ..

# Add title.
echo -e "# Compared rml_kgc tests - LOG\n\n" >> "./$log_file"

# Compare high-level.
echo -e "## Differences (high-level) between current tests and remote tests: \n\n" >> "./$log_file"

base1="src/test/resources/rml_kgc/spec"
base2="remote_tests/resources/rml_kgc/spec"
diff_output=$(diff <(file_list "$base1") <(file_list "$base2"))
if [ -n "$diff_output" ]; then
  {
    echo "**Differences Detected!**"
    echo '```diff'
    echo -e "$diff_output"
    echo -e '```\n'
  } >> "./$log_file"
fi
report_high_level_differences "$base1" "$base2" \
  "src/test/resources/rml_kgc/test-cases/spec-adaptations"

# Compare low-level.
echo -e "## Differences (low-level) between current tests and remote tests: \n" >> "./$log_file"

for name in "${modules[@]}"; do
  base1="src/test/resources/rml_kgc/spec/$name"
  base2="remote_tests/resources/rml_kgc/spec/$name"
  for subdir in "$base1"/*; do
    subdirname=$(basename "$subdir")
    if [ -d "$base2/$subdirname" ]; then
      diff_output=$(diff -rwB -X "$exclude_file" "$base1/$subdirname" "$base2/$subdirname")
      if [ -n "$diff_output" ]; then
        {
          echo -e "**Differences Detected: $name - $subdirname**\n"
          echo '```diff'
          echo "$diff_output"
          echo -e '```\n'
        } >> "./$log_file"
      fi
    fi
  done
done

# Clean up.
echo -e "_Removing remote rml_kgc tests..._\n\n" >> "./$log_file"
rm -rf remote_tests/

# Print info
echo -e "## Consulted rml_kgc repositories and commits: \n" >> "./$log_file"
for key in "${!repositories[@]}"; do
  {
    echo "**Repository/commit of module $key:**"
    echo "${repositories[$key]}"
    echo " & "
    echo "${commits[$key]}"
    echo ""
  } >> "./$log_file"
done

# rmlio

modules=(core fno)
repositories=()
commit=()

# Create temporary directory structure
mkdir -p remote_tests/resources/rmlio/spec/
cd remote_tests

# rmlio/core
name="${modules[0]}"
repo="https://github.com/kg-construct/rml-test-cases"
mkdir -p resources/rmlio/spec/$name

if [[ -d $name ]]; then #Should not happen...
  cd $name
  git pull
  cd -
else
  git clone --filter=blob:none --sparse $repo $name
  cd $name
  git sparse-checkout set test-cases
  commit=$(git rev-parse HEAD)
  repositories["$name"]="${repo}"
  commits["$name"]="$(date +%F\ %T): ${repo}/commit/${commit}"
  cd -
fi

#Moving all test cases according given directory structure.
find $name -type d | grep "RML" | xargs -I '{}' cp -r {} resources/rmlio/spec/$name/


# rmlio/fno
name="${modules[1]}"
repo="https://github.com/RMLio/rml-fno-test-cases"
mkdir -p resources/rmlio/spec/$name

if [[ -d $name ]]; then #Should not happen...
  cd $name
  git pull
  cd -
else
  git clone $repo $name
  cd $name
  commit=$(git rev-parse HEAD)
  repositories["$name"]="${repo}"
  commits["$name"]="$(date +%F\ %T): ${repo}/commit/${commit}"
  cd -
fi

#Moving all test cases according given directory structure.
find $name -type d | grep "RML" | xargs -I '{}' cp -r {} resources/rmlio/spec/$name/

# Step out of remote_tests.
cd ..

# Compare high-level.
echo -e "## Differences (high-level) between current rmlio tests and remote rmlio tests: \n\n" >> "./$log_file"

base1="src/test/resources/rmlio/spec"
base2="remote_tests/resources/rmlio/spec"
diff_output=$(diff <(file_list "$base1") <(file_list "$base2"))
if [ -n "$diff_output" ]; then
  {
    echo "**Differences Detected!**"
    echo '```diff'
    echo -e "$diff_output"
    echo -e '```\n'
  } >> "./$log_file"
fi
report_high_level_differences "$base1" "$base2" \
  "src/test/resources/rmlio/test-cases/spec-adaptations"

# Compare low-level.
echo -e "## Differences (low-level) between current rmlio tests and remote rmlio tests: \n" >> "./$log_file"

for name in "${modules[@]}"; do
  base1="src/test/resources/rmlio/spec/$name"
  base2="remote_tests/resources/rmlio/spec/$name"
  for subdir in "$base1"/*; do
    subdirname=$(basename "$subdir")
    if [ -d "$base2/$subdirname" ]; then
      diff_output=$(diff -rwB -X "$exclude_file" "$base1/$subdirname" "$base2/$subdirname")
      if [ -n "$diff_output" ]; then
        {
          echo -e "**Differences Detected: $name - $subdirname**\n"
          echo '```diff'
          echo "$diff_output"
          echo -e '```\n'
        } >> "./$log_file"
      fi
    fi
  done
done

# Clean up.
echo -e "_Removing remote rmlio tests..._\n\n" >> "./$log_file"
rm -rf remote_tests/

# Print info
echo -e "## Consulted rmlio repositories and commits: \n" >> "./$log_file"
for key in "${!repositories[@]}"; do
  {
    echo "**Repository/commit of module $key:**"
    echo "${repositories[$key]}"
    echo " & "
    echo "${commits[$key]}"
    echo ""
  } >> "./$log_file"
done
