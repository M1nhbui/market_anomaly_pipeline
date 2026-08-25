#!/usr/bin/env bash
#
# Run the local Spark test suite.
#
# Usage:  ./scripts/run_tests.sh
#
set -euo pipefail

cd "$(dirname "$0")/.."

# Spark's driver resolves the machine's hostname at startup and dies with an
# unhelpful "JAVA_GATEWAY_EXITED" if it cannot. Common on VMs, containers, and
# laptops with unusual hostnames. Pinning to loopback sidesteps it entirely and has
# no downside for local-mode Spark.
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export SPARK_LOCAL_HOSTNAME="${SPARK_LOCAL_HOSTNAME:-localhost}"

# Quieten Spark's startup banner.
export PYSPARK_SUBMIT_ARGS="--driver-java-options=-Dlog4j.rootCategory=ERROR pyspark-shell"

python3 -m pytest tests/ -v "$@"
