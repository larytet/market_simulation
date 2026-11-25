#!/bin/sh
# Builds the images, runs the simulator and the sample algorithm, checks the fill stream, cleans up.
set -eu
cd "$(dirname "$0")/.."
rm -rf e2e/out && mkdir -p e2e/out
trap 'docker compose -f docker-compose.e2e.yml down -v --remove-orphans >/dev/null 2>&1 || true' EXIT
docker compose -f docker-compose.e2e.yml up --build --abort-on-container-exit --exit-code-from algo
echo "--- fill stream (e2e/out/fills.csv)"
cat e2e/out/fills.csv
# the stream file holds the header and the two fills of the exact model
test "$(wc -l < e2e/out/fills.csv)" -eq 3
grep -q ',O2,TEST,1,.*,10.02,50,R,1.0000,1.0000,exact,main' e2e/out/fills.csv
grep -q ',O1,TEST,1,.*,10,100,A,1.0000,1.0000,exact,main' e2e/out/fills.csv
echo "E2E PASS"
