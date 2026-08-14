#!/usr/bin/env bash
set -euo pipefail

input_id="${1:-}"
config_path="${2:-configs/replay.yaml}"

if [[ -z "$input_id" ]]; then
  echo "usage: scripts/replay_compare.sh INPUT_ID [CONFIG_PATH]" >&2
  exit 2
fi

if [[ ! "$input_id" =~ ^[0-9]+$ ]]; then
  echo "invalid input_id: $input_id" >&2
  exit 2
fi

record_path_dir="$(
  awk '
    /^mode:[[:space:]]*$/ { in_mode = 1; next }
    in_mode && /^[^[:space:]]/ { in_mode = 0 }
    in_mode && $1 == "record_path_dir:" {
      value = $0
      sub(/^[[:space:]]*record_path_dir:[[:space:]]*/, "", value)
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/^"|"$/, "", value)
      print value
      exit
    }
  ' "$config_path"
)"

if [[ -z "$record_path_dir" ]]; then
  echo "missing mode.record_path_dir in $config_path" >&2
  exit 2
fi

output_root="$record_path_dir/$input_id/output"
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

echo "compared input_id=$input_id baseline=0001 outputs=$compared"
