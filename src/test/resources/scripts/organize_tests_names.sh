#!/usr/bin/env bash

# ===================================================================== #
# Bash script to pretty print test cases name into the following two categories:
#
# 1) Errors not expected from the engine
# 2) Errors are expected from the engine
#
# Author: Sitt Min Oo
# ===================================================================== #

function separator() {
	local char=$1
	if [[ -z "${1+x}" ]]; then
		char="="
	fi
	echo ""
	printf "${char}%.0s" $(seq 1 63)
	echo ""
	echo ""
}

__ScriptVersion="v0.1"

#===  FUNCTION  ================================================================
#         NAME:  usage
#  DESCRIPTION:  Display usage information.
#===============================================================================
function usage() {
	echo "Usage :  $0 [options] <folder>

    Options:
    -h|help       Display this message
    -v|version    Display script version"

} # ----------  end of function usage  ----------

#-----------------------------------------------------------------------
#  Handle command line arguments
#-----------------------------------------------------------------------

while getopts ":hv" opt; do
	case $opt in

	h | help)
		usage
		exit 0
		;;

	v | version)
		echo "$0 -- Version $__ScriptVersion"
		exit 0
		;;

	*)
		echo -e "\n  Option does not exist : $OPTARG\n"
		usage
		exit 1
		;;

	esac # --- end of case ---
done
shift $(($OPTIND - 1))

FOLDER=$1

if [[ ! -d $FOLDER ]]; then
	echo "Given folder \"$FOLDER\" does not exists!"
	usage
	exit 1
fi

POSITIVE=()
NEGATIVE=()

for item in $(find $FOLDER -name "RML*" -type d | sort); do
	test_case="${item##*/}"
	readme_file=$(find $item -name "README*" -type f)
	grep -i "error.*expected.*yes.*" $readme_file 2>&1 >/dev/null
	if [[ $? -eq 0 ]]; then
		NEGATIVE+=($test_case)
	else
		POSITIVE+=($test_case)
	fi
done

echo "Positive test cases:"
for item in ${POSITIVE[@]}; do
	echo $item
done
separator

echo "Negative test cases:"
for item in ${NEGATIVE[@]}; do
	echo $item
done
separator
