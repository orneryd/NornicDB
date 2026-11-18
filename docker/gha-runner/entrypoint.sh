#!/usr/bin/env bash
set -euo pipefail

mkdir -p /runner
if [[ ! -f /runner/config.sh ]]; then
  cp -a /opt/actions-runner/. /runner/
fi
chown -R runner:runner /runner
cd /runner

if [[ ! -f .runner ]]; then
  : "${GITHUB_URL:?GITHUB_URL is required}"
  : "${RUNNER_TOKEN:?RUNNER_TOKEN is required for first registration}"
  : "${RUNNER_NAME:?RUNNER_NAME is required}"
  gosu runner ./config.sh --unattended --url "$GITHUB_URL" --token "$RUNNER_TOKEN" \
    --name "$RUNNER_NAME" --labels nornic-trusted --no-default-labels \
    --work _work --replace
fi

unset RUNNER_TOKEN
exec gosu runner ./run.sh
