#!/usr/bin/env bash

set -u

API_URL="${API_URL:-http://127.0.0.1:8000}"
PROJECT_DIR="$(pwd)"

print_header() {
  printf '\n== %s ==\n' "$1"
}

safe_git() {
  if command -v git >/dev/null 2>&1; then
    "$@" 2>/dev/null || true
  else
    printf 'git unavailable\n'
  fi
}

show_env_matches() {
  local env_file=""
  if [ -f ".env" ]; then
    env_file=".env"
  elif [ -f ".env.production" ]; then
    env_file=".env.production"
  elif [ -f ".env.local" ]; then
    env_file=".env.local"
  fi

  if [ -z "$env_file" ]; then
    printf 'no env file found in %s\n' "$PROJECT_DIR"
    return
  fi

  printf 'env_file=%s\n' "$env_file"
  grep -E '^(OPENAI_ENABLED|OPENAI_MODEL|DEMO_MODE|GREENHOUSE_ENABLED|SEARCH_DISCOVERY_ENABLED|ENABLE_SCHEDULER)=' "$env_file" || true
  if grep -q '^OPENAI_API_KEY=' "$env_file"; then
    if grep -Eq '^OPENAI_API_KEY=.+$' "$env_file"; then
      printf 'OPENAI_API_KEY=<set>\n'
    else
      printf 'OPENAI_API_KEY=<missing>\n'
    fi
  else
    printf 'OPENAI_API_KEY=<missing>\n'
  fi
}

show_processes() {
  if command -v pgrep >/dev/null 2>&1; then
    pgrep -af 'uvicorn api.main:app|scripts/run_worker.py|streamlit run ui/app.py' || printf 'no matching processes found\n'
  else
    ps aux | grep -E 'uvicorn api.main:app|scripts/run_worker.py|streamlit run ui/app.py' | grep -v grep || printf 'no matching processes found\n'
  fi
}

show_logs() {
  if [ -d "logs" ]; then
    find logs -maxdepth 1 -type f | sort | while read -r log_file; do
      print_header "tail ${log_file}"
      tail -n 100 "$log_file" || true
    done
  else
    printf 'logs directory not found\n'
  fi
}

print_header "pwd"
pwd

print_header "git rev-parse --short HEAD"
safe_git git rev-parse --short HEAD

print_header "git status --short"
safe_git git status --short

print_header "process list"
show_processes

print_header "env flags"
show_env_matches

print_header "curl /autonomy-status"
curl -s "${API_URL}/autonomy-status" || printf 'curl failed\n'
printf '\n'

print_header "curl /health"
curl -s "${API_URL}/health" || printf 'curl failed\n'
printf '\n'

print_header "curl /runtime-control"
curl -s "${API_URL}/runtime-control" || printf 'curl failed\n'
printf '\n'

print_header "curl POST /runtime-control action=run_once"
curl -s -X POST "${API_URL}/runtime-control" \
  -H 'Content-Type: application/json' \
  -d '{"action":"run_once"}' || printf 'curl failed\n'
printf '\n'

print_header "curl /opportunities"
curl -s "${API_URL}/opportunities?freshness_window_days=14" || printf 'curl failed\n'
printf '\n'

print_header "recent logs"
show_logs
