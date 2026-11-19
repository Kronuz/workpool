#!/bin/bash
# Runs a work_group test source N times beside CPU hogs and counts failures.
#   test/load_fairness.sh [test.cc] [runs] [hogs]   (from the repository root)
set -u
SRC=${1:-test/work_group.cc}
RUNS=${2:-50}
HOGS=${3:-$(( $(getconf _NPROCESSORS_ONLN) * 2 ))}
CXX=${CXX:-c++}
W=$(mktemp -d)
mkdir "$W/inc" && sed -E 's/^#cmakedefine ([A-Z_]+).*/#define \1 1/' config.h.in >"$W/inc/config.h"
$CXX -std=c++20 -O1 -I"$W/inc" -I. -Itest "$SRC" thread.cc -o "$W/bin" -pthread || exit 2
pids=()
for ((i = 0; i < HOGS; ++i)); do (while :; do :; done) & pids+=($!); done
fail=0
for ((i = 1; i <= RUNS; ++i)); do
	"$W/bin" >"$W/out" 2>&1 || { fail=$((fail + 1)); grep -m1 -E 'failed|Assert' "$W/out"; }
	grep -h 'weight 2 against 1' "$W/out"
done
kill "${pids[@]}" 2>/dev/null
rm -rf "$W"
echo "failures: $fail of $RUNS (hogs: $HOGS)"
[ "$fail" -eq 0 ]
