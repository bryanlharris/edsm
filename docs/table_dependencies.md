# Table dependencies

Silver tables can declare dependencies on other silver tables using an optional `requires` field. During ingestion, tables without dependencies run in a `silver_parallel_loop` while dependent tables are processed later by a `silver_sequential_loop` that honors the `requires` relationships.

## Example

```
layer_02_silver/systems.json      -> {}
layer_02_silver/stations.json     -> {"requires": ["systems"]}

silver_parallel_loop:  systems
silver_sequential_loop: stations
```
