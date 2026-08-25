# VodHunter fingerprint benchmark

This experiment compares three audio-retrieval approaches against one Twitch VOD without writing to the production database:

- `ast`: VodHunter's existing `MIT/ast-finetuned-audioset-10-10-0.4593` embeddings.
- `nmfp_triplet`: the pretrained NAFP-derived NMFP-triplet neural fingerprint model.
- `audfprint`: classical landmark hashes and offset alignment.

The fixed source is JasonTheWeen Twitch VOD `2848966623`. Generated audio, external repositories, weights, indexes, and reports live under `artifacts/`, which is gitignored.

## What is reproducible

- One canonical 16 kHz mono WAV is streamed directly from Twitch and checksummed.
- Ten 25-second clean queries are sampled deterministically across the VOD.
- Every clean query has an exact source timestamp in `artifacts/manifest.jsonl`.
- All engines return one normalized result schema while retaining engine-specific diagnostics.
- Reports score accepted matches within ±2 and ±5 seconds and score explicit rejection for negatives.

## Prerequisites

The main VodHunter Python environment needs the existing backend dependencies plus `ffmpeg`, `ffprobe`, `yt-dlp`, and Git.

audfprint uses the current Python environment. Its small dependency set is already present in the normal development environment; if needed, install the pinned checkout's `requirements.txt` after setup.

NMFP must use a separate Python 3.11 environment because its released model stack is TensorFlow 2.13. On the RTX 2060 machine:

```bash
python3.11 -m venv experiments/fingerprint_benchmark/artifacts/external/nmfp-venv
experiments/fingerprint_benchmark/artifacts/external/nmfp-venv/bin/pip install -r experiments/fingerprint_benchmark/requirements-nmfp.txt
export VODHUNTER_NMFP_PYTHON="$PWD/experiments/fingerprint_benchmark/artifacts/external/nmfp-venv/bin/python"
```

On Windows, use `Scripts/python.exe` instead of `bin/python`. The adapter also discovers that conventional path automatically. GPU execution requires a TensorFlow 2.13-compatible CUDA 11.8/cuDNN environment; CPU inference works but full-VOD extraction will be considerably slower.

TensorFlow 2.13 does not provide native-Windows GPU support. On a Windows RTX machine, run the entire repository inside WSL2 (Ubuntu) or another Linux environment with NVIDIA CUDA passthrough. Do not use the native Windows interpreter if the goal is GPU indexing.

NMFP code and weights are AGPLv3. They are deliberately isolated here and should not be copied into production without a licensing decision.

## Initial setup

Run commands from `VodHunterSearch`:

```bash
python -m experiments.fingerprint_benchmark.benchmark setup --engine all
python -m experiments.fingerprint_benchmark.benchmark prepare-vod
python -m experiments.fingerprint_benchmark.benchmark generate-clean
python -m experiments.fingerprint_benchmark.benchmark status
python -m experiments.fingerprint_benchmark.benchmark preflight --engine all
```

`setup` pins audfprint and NMFP to commits recorded in `external.py`; it downloads only the NMFP-triplet weights, not any training dataset. `prepare-vod` refuses to start if less than 2 GiB is free and never downloads the video stream.

The AST and NMFP ingesters split long VODs into resumable chunks. AST uses five-minute chunks. NMFP uses ten-minute chunks with a 0.5-second overlap so its fingerprint timeline remains continuous. An interrupted index run reuses completed chunk outputs.

## Index and search

Run one engine while developing:

```bash
python -m experiments.fingerprint_benchmark.benchmark index --engine audfprint
python -m experiments.fingerprint_benchmark.benchmark search --engine audfprint --kind clean
python -m experiments.fingerprint_benchmark.benchmark evaluate
```

Run the complete comparison after every engine works:

```bash
python -m experiments.fingerprint_benchmark.benchmark index --engine all
python -m experiments.fingerprint_benchmark.benchmark search --engine all
python -m experiments.fingerprint_benchmark.benchmark evaluate
```

Or, after the two Python environments are configured and `VODHUNTER_NMFP_PYTHON` is set, run the checked-in sequence:

```bash
bash experiments/fingerprint_benchmark/run_clean_benchmark.sh
```

The latest outputs are:

- `artifacts/results/latest.jsonl`: normalized raw results.
- `artifacts/reports/results.csv`: inspectable query-by-query table.
- `artifacts/reports/summary.json`: machine-readable metrics.
- `artifacts/reports/summary.md`: compact comparison table.

Indexing is cached. Add `--force` only when an index or generated clean clips must be replaced.

## Add the real queries later

TikTok queries require a human-verified VOD start offset for honest evaluation:

```bash
python -m experiments.fingerprint_benchmark.benchmark import-query \
  --kind tiktok \
  --id tiktok_01 \
  --source 'https://www.tiktok.com/@example/video/123' \
  --expected-start 3725.2
```

A known negative does not take a timestamp:

```bash
python -m experiments.fingerprint_benchmark.benchmark import-query \
  --kind no_match \
  --id no_match_01 \
  --source /path/to/negative.mp4
```

URLs and local media are normalized to the same query format. Re-run `search --engine all` and `evaluate` after import; source indexes do not need rebuilding.

## Interpreting the first report

The clean queries validate ingestion, localization, and adapter correctness. The ten real TikToks answer the actual overlay/music/SFX question. Three negatives only provide an early rejection signal and must not be used to overfit confidence thresholds. The CSV therefore preserves the raw best candidate even when the engine rejects it.

## NMFP cut-aware alignment experiment

`search-cuts` is an NMFP-only follow-up experiment for edited clips containing
multiple portions of one VOD. It reuses the existing VOD index and cached query
embeddings. For each query fingerprint it retains the top VOD candidates, groups
consecutive candidates with a stable `vod_time - query_time` offset, rejects
isolated matches, and returns every supported source segment plus unmatched query
ranges.

The TikTok discovery manifest can remain separate from the scored benchmark
manifest:

```bash
python -m experiments.fingerprint_benchmark.benchmark search-cuts \
  --manifest experiments/fingerprint_benchmark/artifacts/tiktok_discovery_manifest.jsonl \
  --output experiments/fingerprint_benchmark/artifacts/results/nmfp_cut_detection.jsonl
```

Initial tunable rules are exposed as CLI flags: `--offset-tolerance`, `--max-gap`,
`--min-support`, `--min-duration`, `--min-density`, `--merge-gap`, and
`--merge-offset-tolerance`. The defaults require six fingerprints spanning at
least four seconds, allow up to two seconds of missing evidence inside a track,
and treat offsets within one second as the same fingerprint track. Neighboring
tracks whose offsets differ by no more than four seconds are merged as one
user-facing moment so small trims do not create redundant results. These are
experiment defaults, not production thresholds.
