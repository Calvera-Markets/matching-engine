#!/usr/bin/env bash
# Runs the wal_switch matrix on loopback xfs and ext4 images (or on existing
# mount points) and appends every run to one CSV.
#
#   scripts/wal_fs_bench.sh --work DIR --image-gb N --out CSV --repeats N \
#     --segment-bytes N --segments N --spares N --rate N --window N \
#     --writers "synthetic wal" --poll-us N \
#     [--mounts "xfs=/mnt/a ext4=/mnt/b"] [--cpus "writer helper journal"]
#
# Without --mounts it needs sudo for losetup/mount. Nothing leaves the host.
set -euo pipefail

die() { echo "wal_fs_bench: $*" >&2; exit 2; }

work= image_gb= out= repeats= segment_bytes= segments= spares= rate= window= mounts= cpus= writers= poll_us=
while [[ $# -gt 0 ]]; do
  [[ $# -ge 2 ]] || die "$1 needs a value"
  case "$1" in
    --work) work=$2 ;;
    --image-gb) image_gb=$2 ;;
    --out) out=$2 ;;
    --repeats) repeats=$2 ;;
    --segment-bytes) segment_bytes=$2 ;;
    --segments) segments=$2 ;;
    --spares) spares=$2 ;;
    --rate) rate=$2 ;;
    --window) window=$2 ;;
    --mounts) mounts=$2 ;;
    --cpus) cpus=$2 ;;
    --writers) writers=$2 ;;
    --poll-us) poll_us=$2 ;;
    *) die "unknown flag $1" ;;
  esac
  shift 2
done
for v in work out repeats segment_bytes segments spares rate window writers poll_us; do
  [[ -n "${!v}" ]] || die "missing --${v//_/-}"
done

cargo build --release --bench wal_switch >&2
bin=$(cargo bench --bench wal_switch --no-run --message-format=json 2>/dev/null \
  | python3 -c 'import sys,json
for l in sys.stdin:
    m=json.loads(l)
    if m.get("reason")=="compiler-artifact" and m["target"]["name"]=="wal_switch" and m.get("executable"):
        print(m["executable"])')
[[ -x "$bin" ]] || die "bench binary not found"

declare -A dirs
cleanup() { :; }
if [[ -n "$mounts" ]]; then
  for kv in $mounts; do dirs[${kv%%=*}]=${kv#*=}; done
else
  [[ -n "$image_gb" ]] || die "missing --image-gb (or pass --mounts)"
  mkdir -p "$work"
  mounted=()
  cleanup() { for m in "${mounted[@]}"; do sudo umount "$m" || true; done; }
  trap cleanup EXIT
  for fs in xfs ext4; do
    img="$work/$fs.img" mnt="$work/mnt-$fs"
    mkdir -p "$mnt"
    truncate -s "${image_gb}G" "$img"
    case $fs in
      xfs) mkfs.xfs -q -f "$img" ;;
      ext4) mkfs.ext4 -q -F "$img" ;;
    esac
    sudo mount -o loop "$img" "$mnt"
    sudo chown "$(id -u):$(id -g)" "$mnt"
    mounted+=("$mnt")
    dirs[$fs]=$mnt
  done
fi

cpu_flags=()
if [[ -n "$cpus" ]]; then
  read -r cw ch cj <<<"$cpus"
  cpu_flags=(--cpu-writer "$cw" --cpu-helper "$ch" --cpu-journal "$cj")
fi

for r in $(seq 1 "$repeats"); do
  for fs in "${!dirs[@]}"; do
    for writer in $writers; do
      for prep in hot sparse prealloc prefault both; do
        [[ $writer == wal && $prep == hot ]] && continue
        for mode in inline threaded; do
          "$bin" --dir "${dirs[$fs]}" --segment-bytes "$segment_bytes" --segments "$segments" \
            --spares "$spares" --rate "$rate" --window "$window" --prep "$prep" --mode "$mode" \
            --writer "$writer" --poll-us "$poll_us" --label "run$r" --out "$out" \
            "${cpu_flags[@]}" | tail -n 1
        done
      done
    done
  done
done
