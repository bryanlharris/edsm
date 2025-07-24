#!/usr/bin/bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <profile> <workspace-destination>" >&2
    exit 1
fi

profile="$1"
dest="$2"

if ! command -v databricks > /dev/null 2>&1; then
    echo "Error: databricks CLI not found" >&2
    exit 1
fi

# Import current directory to the workspace, overwriting existing items

databricks --profile "$profile" workspace import_dir -o "$(pwd)" "$dest"

