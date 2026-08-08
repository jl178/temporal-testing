#!/usr/bin/env bash
# End-to-end: restore/build, start worker, execute workflow, assert result.
set -euo pipefail
cd "$(dirname "$0")"

dotnet build -v quiet --nologo Worker/Worker.csproj
dotnet build -v quiet --nologo Starter/Starter.csproj

dotnet run --no-build --project Worker &
WORKER_PID=$!
trap 'kill $WORKER_PID 2>/dev/null || true' EXIT
sleep 3

dotnet run --no-build --project Starter
