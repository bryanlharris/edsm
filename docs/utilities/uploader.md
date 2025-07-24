# Workspace uploader utility

This script uploads files in the current directory to a Databricks workspace.

## `utilities/uploader.sh`

- Accepts a Databricks CLI profile name and a destination workspace path.
- Runs `databricks --profile <profile> workspace import-dir <current directory> <destination> --overwrite`.
- Overwrites existing notebooks in the workspace.
- Uses `rsync` to exclude the `.git` folder before uploading.

