#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./_ci-common.sh
source "${SCRIPT_DIR}/_ci-common.sh"

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <project-dir>" >&2
  exit 1
fi

FILTER_POOL=(
  "STATUS = '200'"
  "STATUS >= '300'"
  "STATUS >= '400'"
  "(METHOD = 'GET') || (METHOD = 'POST')"
  "(METHOD = 'POST' || METHOD = 'PUT') || STATUS = '200'"
  "!(STATUS = '202')"
  "RESPONSE.CONTENT-TYPE = 'application/json'"
  "REQUEST-BODY.CONTENT-TYPE = 'application/json'"
)

FILTER_INDEX=$((RANDOM % ${#FILTER_POOL[@]}))
export FILTER="${FILTER_POOL[$FILTER_INDEX]}"

PROJECT_ARG="$1"

if [[ ! -d "$PROJECT_ARG" ]]; then
  echo "Project directory not found: $PROJECT_ARG" >&2
  exit 1
fi

PROJECT_DIR="$(cd "$PROJECT_ARG" && pwd)"
PROJECT_NAME="$(basename "$PROJECT_DIR")"

if [[ ! -f "$PROJECT_DIR/specmatic.yaml" ]]; then
  echo "specmatic.yaml not found in $PROJECT_DIR" >&2
  exit 1
fi
cd "$PROJECT_DIR"

init_specmatic_cmd
init_colors

if ! command -v yq >/dev/null 2>&1; then
  echo "yq is required to parse specmatic.yaml" >&2
  exit 1
fi

mock_pid=""
compose_logs_pid=""
compose_file=""
compose_started="false"
COMPOSE_CMD=()

init_compose() {
  local candidate

  for candidate in "docker-compose.yaml" "docker-compose.yml" "compose.yaml" "compose.yml"; do
    if [[ -f "${candidate}" ]]; then
      compose_file="${candidate}"
      break
    fi
  done

  if [[ -z "${compose_file}" ]]; then
    return
  fi

  COMPOSE_CMD=(docker compose)
}

start_compose() {
  if [[ -z "${compose_file}" ]]; then
    return
  fi

  echo "${C_BLUE}Starting docker compose from ${PROJECT_DIR}/${compose_file}${C_RESET}"
  if "${COMPOSE_CMD[@]}" -f "${compose_file}" up -d --build 2>&1 | prefix_output "$C_YELLOW" "compose"; then
    compose_started="true"
  else
    echo "${C_RED}docker compose up failed${C_RESET}" >&2
    exit 1
  fi
}

stop_compose() {
  if [[ "${compose_started}" != "true" ]]; then
    return
  fi

  "${COMPOSE_CMD[@]}" -f "${compose_file}" down --remove-orphans 2>&1 | prefix_output "$C_YELLOW" "compose" || true
}

start_compose_logs() {
  if [[ "${compose_started}" != "true" ]]; then
    return
  fi

  "${COMPOSE_CMD[@]}" -f "${compose_file}" logs -f --no-color \
    > >(prefix_output "$C_YELLOW" "compose-log") \
    2> >(prefix_output "$C_YELLOW" "compose-log" >&2) &
  compose_logs_pid=$!
}

send_report() {
  if [[ -z "${SEND_REPORT:-}" ]]; then
    return
  fi


  echo "${C_BLUE}Sending test report to Insights from $(pwd)...${C_RESET}"

  "${SPECMATIC_CMD[@]}" send-report \
    --repo-id=$(gh api 'repos/{owner}/{repo}' --jq .id) \
    --repo-name=$(gh repo view --json name -q .name) \
    --repo-url=$(gh repo view --json url --jq .url) \
    --branch-name main
}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM

  stop_background_process "${mock_pid}"
  stop_background_process "${compose_logs_pid}"
  stop_compose
  if ! send_report; then
    echo "${C_YELLOW}Skipping report upload due to error${C_RESET}" >&2
  fi
  exit "$exit_code"
}

trap cleanup EXIT INT TERM

init_compose
start_compose
start_compose_logs


echo "${C_BLUE}Cleaning up previous build and specmatic state...${C_RESET}"
rm -rf build || true
rm -rf .specmatic || true

echo "${C_BLUE}Project: ${PROJECT_DIR}${C_RESET}"
if [[ "$(yq eval '.dependencies.services | length' specmatic.yaml)" -gt 0 ]]; then
  if [[ "${SPECMATIC_GENERATIVE_TESTS:-}" == "true" ]]; then
    echo "${C_BLUE}SPECMATIC_GENERATIVE_TESTS=true detected; using ${PROJECT_DIR}/specmatic.yaml for dependency mocks${C_RESET}"
  fi

  echo "${C_BLUE}Starting mock from ${PROJECT_DIR}/specmatic.yaml${C_RESET}"
  "${SPECMATIC_CMD[@]}" mock \
    > >(prefix_output "$C_CYAN" "mock") \
    2> >(prefix_output "$C_CYAN" "mock" >&2) &
  mock_pid=$!
  sleep 3
else
  echo "${C_YELLOW}No dependencies.services in ${PROJECT_DIR}/specmatic.yaml; skipping mock startup${C_RESET}"
fi

echo "${C_BLUE}Running test from ${PROJECT_DIR}/specmatic.yaml${C_RESET}"
if "${SPECMATIC_CMD[@]}" test 2>&1 | prefix_output "$C_BLUE" "test"; then
  echo "${C_GREEN}RESULT: PASS (${PROJECT_NAME}: ${PROJECT_DIR})${C_RESET}"
  test_exit=0
else
  test_exit=$?
  echo "${C_RED}RESULT: FAIL (${PROJECT_NAME}: ${PROJECT_DIR}, exit ${test_exit})${C_RESET}"
fi

exit "$test_exit"
