#!/usr/bin/env bash
set -euo pipefail

python -m experiments.fingerprint_benchmark.benchmark setup --engine all
python -m experiments.fingerprint_benchmark.benchmark preflight --engine all
python -m experiments.fingerprint_benchmark.benchmark prepare-vod
python -m experiments.fingerprint_benchmark.benchmark generate-clean
python -m experiments.fingerprint_benchmark.benchmark index --engine all
python -m experiments.fingerprint_benchmark.benchmark search --engine all --kind clean
python -m experiments.fingerprint_benchmark.benchmark evaluate
