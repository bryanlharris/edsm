# Inspect checkpoint folder utility

This notebook helps debug streaming jobs by printing the batch to version mapping for a selected silver table.

## `utilities/inspect_checkpoint_folder.ipynb`

- Lists JSON configuration files from `layer_02_silver` and creates a dropdown widget of available tables.
- Loads the settings for the chosen table, applies defaults with `apply_job_type`, and passes them to `functions.utility.inspect_checkpoint_folder`.
- The defaults include write stream options with the checkpoint folder when `simple_settings` is true.
- The function reads each offset file in the Delta checkpoint folder and prints the silver batch ID alongside the corresponding bronze version.
- Includes an example SQL cell that shows `describe history edsm.bronze.bodies7days`.

