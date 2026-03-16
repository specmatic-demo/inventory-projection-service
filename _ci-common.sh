#!/usr/bin/env bash

init_specmatic_cmd() {
  if command -v specmatic-enterprise >/dev/null 2>&1; then
    SPECMATIC_CMD=(specmatic-enterprise)
    return
  fi

  local jar_path="${HOME}/.specmatic/specmatic-enterprise.jar"
  if [[ ! -f "$jar_path" ]]; then
    echo "specmatic-enterprise not found in PATH and jar not found at $jar_path" >&2
    exit 1
  fi

  local java_opts="${JAVA_OPTS:-}"
  # shellcheck disable=SC2206
  SPECMATIC_CMD=(java -Djava.awt.headless=true $java_opts -jar "$jar_path")
}

init_colors() {
  if [[ -t 1 ]]; then
    C_RESET=$'\033[0m'
    C_BLUE=$'\033[34m'
    C_CYAN=$'\033[36m'
    C_GREEN=$'\033[32m'
    C_RED=$'\033[31m'
    C_YELLOW=$'\033[33m'
  else
    C_RESET=''
    C_BLUE=''
    C_CYAN=''
    C_GREEN=''
    C_RED=''
    C_YELLOW=''
  fi
}

prefix_output() {
  local color="$1"
  local label="$2"
  sed -u "s/^/${color}[${label}]${C_RESET} /"
}

stop_background_process() {
  local pid="${1:-}"
  if [[ -n "$pid" ]]; then
    stop_process_tree "$pid"
    wait "$pid" >/dev/null 2>&1 || true
  fi
}

stop_process_tree() {
  local pid="$1"
  local child_pid

  if ! kill -0 "$pid" >/dev/null 2>&1; then
    return
  fi

  while IFS= read -r child_pid; do
    [[ -n "$child_pid" ]] || continue
    stop_process_tree "$child_pid"
  done < <(pgrep -P "$pid" 2>/dev/null || true)

  kill "$pid" >/dev/null 2>&1 || true
  sleep 0.2
  kill -9 "$pid" >/dev/null 2>&1 || true
}
