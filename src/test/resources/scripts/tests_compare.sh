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

cd ../../..

#Create log-file.
log_file="test/resources/scripts/diff_log.md"
> "$log_file"

# rml_kgc

#Create temporary directory structure
mkdir -p remote_tests/resources/spec/rml_kgc/
cd remote_tests
declare -A repositories
declare -A commits

modules=(rml-core rml-io rml-cc rml-fnml rml-star rml-lv)

for name in "${modules[@]}"; do

  #Cloning test-cases folders from different repositories.
  mkdir -p resources/spec/rml_kgc/$name
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
  find $name -type d | grep "RML" | xargs -I '{}' cp -r {} resources/spec/rml_kgc/$name/
done

# Step out of remote_tests.
cd ..

# Add title.
echo -e "# Compared rml_kgc tests - LOG\n\n" >> "./$log_file"

# Compare high-level.
echo -e "## Differences (high-level) between current tests and remote tests: \n\n" >> "./$log_file"

base1="test/resources/spec/rml_kgc"
base2="remote_tests/resources/spec/rml_kgc"
diff_output=$(diff <(cd "$base1" && find . | sort) <(cd "$base2" && find . | sort))
if [ -n "$diff_output" ]; then
  {
    echo "**Differences Detected!**"
    echo '```diff'
    echo -e "$diff_output"
    echo -e '```\n'
  } >> "./$log_file"
fi

# Compare low-level.
echo -e "## Differences (low-level) between current tests and remote tests: \n" >> "./$log_file"

for name in "${modules[@]}"; do
  base1="test/resources/spec/rml_kgc/$name"
  base2="remote_tests/resources/spec/rml_kgc/$name"
  for subdir in "$base1"/*; do
    subdirname=$(basename "$subdir")
    if [ -d "$base2/$subdirname" ]; then
      diff_output=$(diff -rwB "$base1/$subdirname" "$base2/$subdirname")
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
mkdir -p remote_tests/resources/spec/rmlio/
cd remote_tests

# rmlio/core
name="${modules[0]}"
repo="https://github.com/kg-construct/rml-test-cases"
mkdir -p resources/spec/rmlio/$name

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
find $name -type d | grep "RML" | xargs -I '{}' cp -r {} resources/spec/rmlio/$name/


# rmlio/fno
name="${modules[1]}"
repo="https://github.com/RMLio/rml-fno-test-cases"
mkdir -p resources/spec/rmlio/$name

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
find $name -type d | grep "RML" | xargs -I '{}' cp -r {} resources/spec/rmlio/$name/

# Step out of remote_tests.
cd ..

# Compare high-level.
echo -e "## Differences (high-level) between current rmlio tests and remote rmlio tests: \n\n" >> "./$log_file"

base1="test/resources/spec/rmlio"
base2="remote_tests/resources/spec/rmlio"
diff_output=$(diff <(cd "$base1" && find . | sort) <(cd "$base2" && find . | sort))
if [ -n "$diff_output" ]; then
  {
    echo "**Differences Detected!**"
    echo '```diff'
    echo -e "$diff_output"
    echo -e '```\n'
  } >> "./$log_file"
fi

# Compare low-level.
echo -e "## Differences (low-level) between current rmlio tests and remote rmlio tests: \n" >> "./$log_file"

for name in "${modules[@]}"; do
  base1="test/resources/spec/rmlio/$name"
  base2="remote_tests/resources/spec/rmlio/$name"
  for subdir in "$base1"/*; do
    subdirname=$(basename "$subdir")
    if [ -d "$base2/$subdirname" ]; then
      diff_output=$(diff -rwB "$base1/$subdirname" "$base2/$subdirname")
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
