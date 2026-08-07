#!/bin/sh
set -eu

# An optional procfs path keeps the script deterministic under contract tests.
# Docker invokes the script without arguments and therefore uses the real tree.
PROC_ROOT="${1:-/proc}"
API_PORT_VALUE="${API_PORT:-8000}"
READINESS_URL="http://127.0.0.1:${API_PORT_VALUE}/api/v1/health/ready"

DSA_PID=""
DSA_CMDLINE=""

is_live_process() {
    pid="$1"
    status_file="$PROC_ROOT/$pid/status"

    if ! kill -0 "$pid" 2>/dev/null; then
        return 1
    fi
    if [ -r "$status_file" ]; then
        state=""
        while read -r status_key status_value _status_rest; do
            if [ "$status_key" = "State:" ]; then
                state="$status_value"
                break
            fi
        done < "$status_file"
        if [ "$state" = "Z" ]; then
            return 1
        fi
    fi
    return 0
}

find_dsa_process() {
    for process_dir in "$PROC_ROOT"/[0-9]*; do
        [ -d "$process_dir" ] || continue
        pid="${process_dir##*/}"
        cmdline_file="$process_dir/cmdline"
        [ -r "$cmdline_file" ] || continue

        if ! cmdline="$(tr '\000' ' ' < "$cmdline_file" 2>/dev/null)"; then
            continue
        fi
        case " $cmdline " in
            *" main.py "*|*"/main.py "*|*" server.py "*|*"/server.py "*|*" webui.py "*|*"/webui.py "*|*"uvicorn "*" server:app "*|*"gunicorn "*" server:app "*)
                if is_live_process "$pid"; then
                    DSA_PID="$pid"
                    DSA_CMDLINE="$cmdline"
                    return 0
                fi
                ;;
        esac
    done
    return 1
}

is_api_mode() {
    case " $DSA_CMDLINE " in
        *" --serve "*|*" --serve-only "*|*" --webui "*|*" --webui-only "*|*" server.py "*|*"/server.py "*|*" webui.py "*|*"/webui.py "*|*"uvicorn "*" server:app "*|*"gunicorn "*" server:app "*)
            return 0
            ;;
    esac
    case "${WEBUI_ENABLED:-false}" in
        [Tt][Rr][Uu][Ee]) return 0 ;;
    esac
    return 1
}

if ! find_dsa_process; then
    printf '%s\n' "DSA healthcheck failed: no live DSA process found" >&2
    exit 1
fi

if is_api_mode; then
    exec curl --fail --silent --show-error --max-time 8 "$READINESS_URL"
fi

# Non-HTTP scheduler/CLI mode has no readiness endpoint. Success here means a
# recognized, non-zombie DSA process is still alive; arbitrary PID 1 commands
# do not satisfy the probe.
exit 0
