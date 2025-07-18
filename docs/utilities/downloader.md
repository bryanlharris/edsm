# Downloader utilities

This folder contains two files used to fetch the nightly EDSM data dumps.

## `utilities/downloader.sh`

- Creates working directories under `/tmp/data` and `/Volumes/edsm/bronze/landing`.
- Defines a `download` function that takes a timestamp marker and one or more URLs.
- Downloads each URL with `wget`, decompresses it with `gunzip` and cleans the JSON using `sed`.
- Copies the result into the landing `data/<date>` directory.
- Skips downloading when a marker directory already exists.
- Example calls download yearly, weekly and daily dumps.

## `utilities/downloader.ipynb`

- Single cell notebook that executes `%sh` and runs `./downloader.sh`.
- Used by the Databricks job's `downloader` task to initiate downloads.
