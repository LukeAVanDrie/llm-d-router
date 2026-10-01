#!/bin/bash
# The self-reuse check: 6 policies x 3 cells x 2 engines x 2 loads, scored against fit and nogate.
#   examples/self_reuse_check.sh [--quick] [OUT_DIR]
# Full: 5 seeds x 10,000 requests (360 runs). --quick: 2 seeds x 2,500 requests (144 runs).
# Override with SEEDS="104 105 ..." REQUESTS=N JOBS=N; PYTHON selects the interpreter.
set -e
cd "$(dirname "$0")/.."
SEEDS=${SEEDS:-"104 105 106 107 108"}
REQUESTS=${REQUESTS:-10000}
if [ "$1" = "--quick" ]; then
  SEEDS="104 105"
  REQUESTS=2500
  shift
fi
OUT=${1:-out/self_reuse_check-$REQUESTS}
PY=${PYTHON:-python}
$PY -m admsim batch -o "$OUT" --seeds $SEEDS --requests "$REQUESTS" ${JOBS:+-j $JOBS}
$PY -m admsim score "$OUT" --baseline fit
$PY -m admsim score "$OUT" --baseline nogate
