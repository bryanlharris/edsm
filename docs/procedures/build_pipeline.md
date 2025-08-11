# Build a New Pipeline

Follow this procedure to spin up a fresh pipeline based on the existing **edsm** project.

## 1. Clone the Repository

```bash
git clone <repository-url> name
cd name
```

## 2. Upload to Databricks

This is the step where you can stop using git if you need to and archive the existing layer JSON settings.

```bash
rsync -av --exclude='.git' name name1
cd name1
for layer in layer_??_*; do
  mkdir -p "$layer/archive"
  mv "$layer"/*.json "$layer/archive/"
done
databricks -p dev auth login
databricks -p dev workspace import-dir . /home/whoeveryouare@example.com/name1
```

## 3. Update Configuration

Edit [`functions/config.py`](../../functions/config.py) to point to your own infrastructure. Ensure the paths end with a trailing `/`.

- `S3_ROOT_LANDING`
- `S3_ROOT_UTILITY`
- `OBJECT_OWNER`

Whatever you put for each S3 path will end up with `/<catalog>/<schema>/<landing|utility>/` at the end of it.

## 4. Create a Bronze JSON Table Config

Go into the bronze folder, which should be empty, and see if you can create your own table json file. You can try using the get-schema.py utility from the databricks-utilities repo to get the file-schema, and fill in the rest. You can look into the archive or examples folders for ideas.

Here are some other to look at for this step:

- docs/bronze.md
- docs/functions/config.md
- docs/functions/read.md
- docs/functions/sanity.md

## 5. Create a Databricks Job

In the Databricks workspace, create a new job. For now, only create the job settings task in the job. You can look at the included YAML file for guidance but essentially you just point your task at the job settings notebook and you should be good.

- Add a task pointing to `00_job_settings.ipynb` to generate job settings.

## 6. Run the Job

Execute `00_job_settings.ipynb`.

You should now see the sanity checking run. It should create schemas, tables, and volumes for you and should change the owner of all objects created.

## 7. Populate More Job Tasks

At this stage, if you've got your raw schema, your empty table created with the sanity check, and your volumes, go ahead and create the bronze loop. Again, look at the included YAML for guidance.

- Include a variable ``job_settings`` that comes from ``{{input}}``.
- Include a variable ``color`` set to the string "bronze" (do not put the double quotes).

## 8. End (for now)

You now have a clean starting point for your own pipeline, with previous EDSM settings archived for reference. If you need history, silver, samples, or gold, you can use the YAML and archived examples from EDSM to figure things out from here.
