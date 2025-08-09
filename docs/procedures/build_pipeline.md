# Build a New Pipeline

Follow this procedure to spin up a fresh pipeline based on the existing **edsm** project.

## 1. Clone the Repository

```bash
git clone <repository-url>
cd edsm
```

## 2. Archive Existing Layer Settings

Each layer directory (`layer_01_bronze`, `layer_02_silver`, `layer_03_gold`, etc.) contains JSON settings files that describe the EDSM pipeline. To start a new pipeline, preserve the old files and begin with empty ones.

For every layer:

```bash
mkdir -p <layer>/archive
mv <layer>/*.json <layer>/archive/
```

Create new JSON settings in each layer to describe the tables for the new pipeline.

## 3. Update Configuration

Edit [`functions/config.py`](../../functions/config.py) to point to your own infrastructure.

- `S3_ROOT_LANDING`
- `S3_ROOT_UTILITY`
- optionally `OBJECT_OWNER`

Ensure the paths end with a trailing `/`.

## 4. Create a Databricks Job

In the Databricks workspace, create a new job that orchestrates the notebooks for your pipeline.

- Add a task pointing to `00_job_settings.ipynb` to generate job settings.
- Add downstream tasks that run the ingestion notebooks (`03_ingest.ipynb` or similar) for each table and layer.
- Configure "for each" task loops or dependent tasks as needed to process all tables.

## 5. Run Job Settings Notebook

Execute `00_job_settings.ipynb` to generate the `job_settings` JSON for each table. Add new tables or modify the settings to fit your data.

## 6. Commit and Push

Commit your new settings, configuration changes and any custom functions to your fork or repository.

```bash
git add .
git commit -m "Add initial pipeline configuration"
git push origin main
```

You now have a clean starting point for your own pipeline, with previous EDSM settings archived for reference.
