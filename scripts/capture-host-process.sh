#!/usr/bin/env sh
# Keep descendant output off the runner's pipes, then replay the complete log.
# The command's exit status, including failure, remains authoritative.
if [ "$#" -lt 2 ]; then
  echo "usage: capture-host-process.sh <log> <command> [args...]" >&2
  exit 2
fi
rsf_capture_log=$1
shift
"$@" > "$rsf_capture_log" 2>&1
rsf_command_status=$?
cat "$rsf_capture_log" || exit $?
exit "$rsf_command_status"
