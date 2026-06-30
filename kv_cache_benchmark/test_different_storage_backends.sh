#!/bin/bash
# test_different_storage_backends.sh — Hazem Awadallah, Kingston Digital
#
# Validates the kv-cache bpftrace device-filter detection + safe fallback and the
# fio-distiller capture warnings across THREE storage backings layered on ONE
# NVMe drive:
#
#   POSIX block : a directory on the drive's own filesystem   -> real dev_t filter
#   NFS         : loopback export of a subdir on the drive     -> synthetic st_dev -> dev=0 + warning
#   S3 / FUSE   : MinIO data on a subdir + s3fs/rclone mount    -> synthetic st_dev -> dev=0 + warning
#
# All three land on the SAME physical NVMe, so the test shows the tracer isolates
# it for POSIX but correctly falls back (and warns) for NFS/S3, where the
# block-layer cannot see the application's I/O.
#
# Usage:   sudo ./test_different_storage_backends.sh /dev/nvmeXn1
# DESTRUCTIVE: reformats the given device (must be unmounted). Set CONFIRM=no for a dry-run.
# Requires: sudo, bpftrace, kv-cache (venv or kv-cache.py), and for NFS/S3:
#           nfs-kernel-server, minio + mc, and s3fs or rclone.
#
# Exit code: 0 if every available backing PASSed (SKIP is allowed when a backing's
#            tooling is absent); non-zero if any backing FAILed its assertion.
set -u

DEV="${1:-}"
[ -z "$DEV" ] && { echo "usage: $0 /dev/nvmeXn1"; exit 2; }
[ -b "$DEV" ] || { echo "ERROR: $DEV is not a block device"; exit 2; }
if findmnt -S "$DEV" >/dev/null 2>&1 || lsblk -nro MOUNTPOINT "$DEV" | grep -q .; then
    echo "ERROR: $DEV (or a partition of it) is mounted — refusing to reformat."; exit 2; fi

DUR="${DUR:-15}"; USERS="${USERS:-50}"; CONFIRM="${CONFIRM:-yes}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"; cd "$SCRIPT_DIR"

# Resolve the kv-cache entry point: venv console script, then PATH, then kv-cache.py.
if [ -x "$SCRIPT_DIR/.venv/bin/kv-cache" ]; then
    KV=("$SCRIPT_DIR/.venv/bin/kv-cache")
elif command -v kv-cache >/dev/null 2>&1; then
    KV=("$(command -v kv-cache)")
else
    KV=(python3 "$SCRIPT_DIR/kv-cache.py")
fi

OUT="$SCRIPT_DIR/backend_test_$(basename "$DEV")"; mkdir -p "$OUT"

BASE=/mnt/kvbe                       # the one drive, mounted here
POSIX_DIR="$BASE/posix"
NFS_EXPORT="$BASE/nfs_export"; NFS_MNT=/mnt/kvbe_nfs
S3_DATA="$BASE/minio_data";    S3_MNT=/mnt/kvbe_s3; S3_BUCKET=kvcache
MINIO_PID=""; declare -A RESULT
log(){ echo -e "\n\033[1m== $* ==\033[0m"; }

teardown(){
    log "teardown"
    mountpoint -q "$S3_MNT" && sudo umount "$S3_MNT" 2>/dev/null
    [ -n "$MINIO_PID" ] && kill "$MINIO_PID" 2>/dev/null
    mountpoint -q "$NFS_MNT" && sudo umount "$NFS_MNT" 2>/dev/null
    [ -f /etc/exports.d/kvbe.exports ] && { sudo rm -f /etc/exports.d/kvbe.exports; sudo exportfs -ra 2>/dev/null; }
    mountpoint -q "$BASE" && sudo umount "$BASE" 2>/dev/null
}
trap teardown EXIT

# Run a short traced kv-cache stress run on $cdir and assert the device filter
# resolved as expected ("block" = real dev_t, "fallback" = dev=0 + a warning).
run_backend(){  # $1=label $2=cache_dir $3=expect(block|fallback)
    local label="$1" cdir="$2" expect="$3"
    local lg="$OUT/$label.log" js="$OUT/$label.json"
    if [ ! -d "$cdir" ] || ! touch "$cdir/.wtest" 2>/dev/null; then
        echo "  SKIP $label: $cdir not writable"; RESULT[$label]="SKIP (mount/setup failed)"; return; fi
    rm -f "$cdir/.wtest"
    echo "  running ${DUR}s traced kv-cache on $label ($cdir) ..."
    "${KV[@]}" --config config.yaml --model llama3.1-8b --num-users "$USERS" --duration "$DUR" \
        --gpu-mem-gb 0 --cpu-mem-gb 0 --max-concurrent-allocs 16 \
        --generation-mode none --cache-dir "$cdir" --seed 42 --performance-profile throughput \
        --enable-latency-tracing --output "$js" --xlsx-output "$OUT/$label.xlsx" > "$lg" 2>&1
    local filt dev warn rwm
    filt=$(grep -m1 "Filter: comm=" "$lg" | sed 's/^[[:space:]]*//')
    dev=$(echo "$filt" | sed -n 's/.*dev=\([0-9]*\).*/\1/p')
    warn=$(grep -ciE "network/object/virtual|no block I/O captured|filter disabled" "$lg")
    rwm=$(python3 -c "import json;print([l.split('=')[1].strip() for l in json.load(open('$js')).get('fio_workload','').splitlines() if l.startswith('rwmixread=')][0])" 2>/dev/null || echo "?")
    echo "    $filt"
    echo "    distilled rwmixread=$rwm ; capture-warnings=$warn"
    if [ "$expect" = "block" ]; then
        [ -n "$dev" ] && [ "$dev" != "0" ] && RESULT[$label]="PASS dev=$dev (real block dev), rwmix=$rwm" \
            || RESULT[$label]="FAIL expected real dev_t, got dev=$dev"
    else
        [ "$dev" = "0" ] && [ "$warn" -ge 1 ] && RESULT[$label]="PASS fallback dev=0 + warning, rwmix=$rwm" \
            || RESULT[$label]="FAIL expected dev=0+warning, got dev=$dev warn=$warn"
    fi
}

# ---------- format + mount the single drive ----------
DEVT=$(python3 -c "import os;s=os.stat('/dev/${DEV##*/}');print((os.major(s.st_rdev)<<20)|os.minor(s.st_rdev))")
log "ONE DRIVE: $DEV  (expected POSIX dev_t=$DEVT)"
[ "$CONFIRM" = "yes" ] || { echo "  CONFIRM=no -> dry-run, not formatting"; exit 0; }
echo "  reformatting $DEV (xfs) and mounting at $BASE ..."
sudo mkfs.xfs -f "$DEV" >/dev/null 2>&1 && sudo mkdir -p "$BASE" && sudo mount "$DEV" "$BASE" \
    && sudo chown "$(id -u):$(id -g)" "$BASE" || { echo "  ERROR: format/mount failed"; exit 1; }
mkdir -p "$POSIX_DIR" "$NFS_EXPORT" "$S3_DATA"; chmod 777 "$NFS_EXPORT"

# ---------- 1) POSIX (directory on the drive) ----------
log "POSIX block backing (dir on $DEV)"
run_backend posix "$POSIX_DIR" block

# ---------- 2) NFS (loopback export of a subdir on the drive) ----------
log "NFS backing (loopback export of $NFS_EXPORT)"
command -v exportfs >/dev/null || sudo apt-get install -y nfs-kernel-server nfs-common >/dev/null 2>&1
if command -v exportfs >/dev/null; then
    sudo mkdir -p /etc/exports.d
    echo "$NFS_EXPORT localhost(rw,sync,no_subtree_check,no_root_squash)" | sudo tee /etc/exports.d/kvbe.exports >/dev/null
    sudo systemctl start nfs-server 2>/dev/null || sudo systemctl start nfs-kernel-server 2>/dev/null
    sudo exportfs -ra
    sudo mkdir -p "$NFS_MNT"
    if sudo mount -t nfs -o vers=4 localhost:"$NFS_EXPORT" "$NFS_MNT" 2>/dev/null \
       || sudo mount -t nfs localhost:"$NFS_EXPORT" "$NFS_MNT" 2>/dev/null; then
        sudo chmod 777 "$NFS_MNT"; run_backend nfs "$NFS_MNT" fallback
    else echo "  NFS mount failed"; RESULT[nfs]="SKIP (nfs mount failed)"; fi
else echo "  no nfs server"; RESULT[nfs]="SKIP (no nfs server)"; fi

# ---------- 3) S3 (MinIO data on a subdir + FUSE mount) ----------
log "S3 backing (MinIO data in $S3_DATA, FUSE mount)"
command -v s3fs >/dev/null || command -v rclone >/dev/null || sudo apt-get install -y s3fs >/dev/null 2>&1
if command -v minio >/dev/null; then
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
        minio server "$S3_DATA" --address 127.0.0.1:9000 > "$OUT/minio.log" 2>&1 &
    MINIO_PID=$!; sleep 4
    mc alias set kvbe http://127.0.0.1:9000 minioadmin minioadmin >/dev/null 2>&1
    mc mb -p "kvbe/$S3_BUCKET" >/dev/null 2>&1
    sudo mkdir -p "$S3_MNT"; FUSE=""
    if command -v s3fs >/dev/null; then
        echo "minioadmin:minioadmin" > /tmp/kvbe_s3pw; chmod 600 /tmp/kvbe_s3pw
        sudo s3fs "$S3_BUCKET" "$S3_MNT" -o url=http://127.0.0.1:9000 -o use_path_request_style \
            -o passwd_file=/tmp/kvbe_s3pw -o allow_other -o umask=0000 2>"$OUT/s3fs.log" && FUSE=s3fs
    elif command -v rclone >/dev/null; then
        rclone config create kvbes3 s3 provider=Minio endpoint=http://127.0.0.1:9000 \
            access_key_id=minioadmin secret_access_key=minioadmin >/dev/null 2>&1
        rclone mount "kvbes3:$S3_BUCKET" "$S3_MNT" --daemon --allow-other --vfs-cache-mode writes 2>"$OUT/rclone.log" && FUSE=rclone
    fi
    if [ -n "$FUSE" ] && mountpoint -q "$S3_MNT"; then run_backend s3 "$S3_MNT" fallback
    else echo "  S3 FUSE mount failed"; RESULT[s3]="SKIP (no s3fs/rclone)"; fi
else echo "  no minio"; RESULT[s3]="SKIP (no minio)"; fi

# ---------- summary ----------
log "RESULTS  (all backings on $DEV)"
rc=0
for k in posix nfs s3; do
    printf "  %-7s %s\n" "$k" "${RESULT[$k]:-<not run>}"
    case "${RESULT[$k]:-}" in FAIL*) rc=1;; esac
done
exit $rc
