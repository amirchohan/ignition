#!/usr/bin/env bash
set -euo pipefail

run_dir="${1:-}"

if [[ -z "$run_dir" ]]; then
  echo "usage: scripts/replay_compare.sh RUN_DIR" >&2
  exit 2
fi

if [[ ! -d "$run_dir" ]]; then
  echo "missing replay run directory: $run_dir" >&2
  exit 2
fi
output_root="$run_dir/output"
baseline="$output_root/0001"

if [[ ! -d "$baseline" ]]; then
  echo "missing replay comparison baseline: $baseline" >&2
  exit 1
fi

shopt -s nullglob
outputs=("$output_root"/[0-9][0-9][0-9][0-9])
compared=0
for output_dir in "${outputs[@]}"; do
  [[ -d "$output_dir" ]] || continue
  output_id="$(basename "$output_dir")"
  [[ "$output_id" != "0001" ]] || continue
  compared=$((compared + 1))

  for endpoint in control.bin gateway.bin market_data.bin; do
    expected="$baseline/$endpoint"
    actual="$output_dir/$endpoint"
    if [[ ! -f "$expected" && ! -f "$actual" ]]; then
      continue
    fi
    if [[ -f "$expected" && ! -f "$actual" ]]; then
      echo "missing replay output: expected=$expected actual=$actual" >&2
      exit 1
    fi
    if [[ ! -f "$expected" && -f "$actual" ]]; then
      echo "missing baseline replay output: expected=$expected actual=$actual" >&2
      exit 1
    fi
    cmp -s "$expected" "$actual" || {
      echo "replay output mismatch: expected=$expected actual=$actual" >&2
      exit 1
    }
  done
done

echo "compared run=$run_dir baseline=0001 outputs=$compared"
