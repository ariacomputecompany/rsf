#!/usr/bin/env sh
# Keep descendant output off the runner's pipes, then replay both complete logs.
# Separate streams prevent peer stderr writes from splitting libtest stdout.
# The command's exit status, including failure, remains authoritative.
if [ "$#" -lt 2 ]; then
  echo "usage: capture-host-process.sh <log> <command> [args...]" >&2
  exit 2
fi
rsf_capture_log=$1
shift
"$@" > "$rsf_capture_log" 2> "$rsf_capture_log.stderr"
rsf_command_status=$?
cat "$rsf_capture_log" "$rsf_capture_log.stderr" || exit $?
exit "$rsf_command_status"
