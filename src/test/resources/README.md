# Information Test Resources-folder

## Scripts
**Script tests_compare.sh:**   
- Downloads the remote RML.io tests (rmlio) from https://github.com/kg-construct/. The RML-KGC suites are run by mapping-test-runner at pinned commits (HANDBOOK: Conformance).
- Compares the remote tests to the local tests.
- Writes to diff_log.md. This file shows all differences between the two test-folders (high-level + low-level).
  This file is also stored in the scripts-folder.
- Remote tests are cleaned up afterwards.

!! Converting .sh-file to unix maybe necessary (dos2unix script.sh). !!
!! Right now, the script works with relative paths, so it must be called from scripts-folder. !!