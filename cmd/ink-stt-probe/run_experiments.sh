#!/usr/bin/env bash
# Runs from any working directory. Never source/echo credentials or enable xtrace.
set -uo pipefail
set +x
probe_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$probe_root" || exit 1
probe_out=/tmp/ink-probe/out
# Keep build-cache/temp writes inside the task's permitted scratch directory.
export GOCACHE=/tmp/ink-probe/go-cache
export GOTMPDIR=/tmp/ink-probe/go-tmp
mkdir -p "$probe_out" "$GOCACHE" "$GOTMPDIR"
go build -o /tmp/ink-probe/ink-stt-probe ./cmd/ink-stt-probe || exit 1
probe_bin=/tmp/ink-probe/ink-stt-probe

run() {
  local name="$1"
  shift
  printf 'Starting %s\n' "$name"
  "$probe_bin" -env-file "$probe_root/.env" -json-out "$probe_out/$name.json" "$@" >"$probe_out/$name.log" 2>&1
  local status=$?
  # The binary retries HTTP upgrade errors. Retry a whole session once only for
  # post-upgrade auth/rate/concurrency errors, retaining the first artifact.
  if python3 - "$probe_out/$name.json" <<'PY'
import json, sys
try:
    data=json.load(open(sys.argv[1]))
except (OSError, ValueError):
    sys.exit(1)
for event in data['events']:
    if event['type'] != 'error':
        continue
    raw=event.get('raw', {})
    text=json.dumps(raw).lower()
    if raw.get('status_code') in (401,403,429) or any(s in text for s in ('concurrency', 'concurrent', 'rate limit', 'rate_limit')):
        sys.exit(0)
sys.exit(1)
PY
  then
    mv "$probe_out/$name.json" "$probe_out/$name.attempt1.json"
    mv "$probe_out/$name.log" "$probe_out/$name.attempt1.log"
    sleep 10
    "$probe_bin" -env-file "$probe_root/.env" -json-out "$probe_out/$name.json" "$@" >"$probe_out/$name.log" 2>&1
    status=$?
  fi
  printf '%s\n' "$status" >"$probe_out/$name.exit-code"
  printf 'Finished %s (exit %s)\n' "$name" "$status"
}

# Launch the 200-second no-audio idle test FIRST, overlapping only this run.
run 11-idle-long -mode auto -model ink-preview -idle-secs 200 -timeout-secs 230 &
probe_idle_pid=$!
trap 'kill "$probe_idle_pid" 2>/dev/null || true' INT TERM
# Wait for the idle probe's flushed startup log so its dial precedes run 1.
for ((i=0; i<100; i++)); do
  [[ -s "$probe_out/11-idle-long.log" ]] && break
  sleep 0.1
done

run 01-auto-en -mode auto -file /tmp/ink-probe/en16k.raw
run 02-auto-hinglish -mode auto -file /tmp/ink-probe/hinglish16k.raw
run 03-auto-hi -mode auto -file /tmp/ink-probe/hi16k.raw
run 04-auto-two-turns -mode auto -file /tmp/ink-probe/twoutt16k.raw
run 05-auto-ink2-en -mode auto -model ink-2 -file /tmp/ink-probe/en16k.raw
run 06-manual-en -mode manual -file /tmp/ink-probe/en16k.raw -finalize
run 07-manual-hinglish -mode manual -file /tmp/ink-probe/hinglish16k.raw -finalize
run 08-whisper-hinglish-hi -mode manual -model ink-whisper -file /tmp/ink-probe/hinglish16k.raw -language hi -finalize
run 09-whisper-en-no-finalize -mode manual -model ink-whisper -file /tmp/ink-probe/en16k.raw -finalize=false
run 10-auto-hinglish-fast-end -mode auto -file /tmp/ink-probe/hinglish16k.raw -turn-end-threshold 0.1 -turn-end-timeout-ms 1500
run 12-idle-keepalive -mode auto -idle-secs 30 -keepalive-text '{"type":"keepalive"}'
run 13-invalid-auto-whisper -mode auto -model ink-whisper -file /tmp/ink-probe/en16k.raw -tail-silence-secs 6 -timeout-secs 30
wait "$probe_idle_pid"
python3 - "$probe_out" <<'PY'
import json, pathlib, sys
files=sorted(p for p in pathlib.Path(sys.argv[1]).glob('[0-9][0-9]-*.json') if '.attempt' not in p.name)
assert len(files) == 13, f'Expected 13 artifacts, got {len(files)}'
for path in files:
    data=json.loads(path.read_text())
    assert data['events'], f'{path.name}: no observations'
    counts={}
    for event in data['events']:
        counts[event['kind']]=counts.get(event['kind'],0)+1
    print(path.name, counts)
print('All 13 experiments have captured observations; HTTP rejections are labeled HTTP, not fabricated WebSocket events.')
PY
