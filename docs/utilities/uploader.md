# Workspace uploader utility

This script uploads files in the current directory to a Databricks workspace.

## `utilities/uploader.sh`

- Accepts a Databricks CLI profile name and a destination workspace path.
- Runs `databricks --profile <profile> workspace import_dir -o <current directory> <destination>`.
- Overwrites existing notebooks in the workspace.

