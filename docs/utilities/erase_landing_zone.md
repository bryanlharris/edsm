# Erase landing zone

This notebook exists to wipe the landing directory used by `Auto Loader`.

## `utilities/erase_landing_zone.ipynb`

- Contains a single `%sh` cell with commented commands.
- When uncommented, it changes to `/Volumes/edsm/bronze/landing` and runs `rm -rf *`.
- Useful for removing all previously downloaded dumps from the landing volume.
