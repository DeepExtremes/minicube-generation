# DeepExtremes Minicube Generation

Contains the instructions on how to create minicubes for the DeepExtremes 
project.

`cube.geojson` contains a template for creating minicubes.
Run `01-config-generation/generate-config.py` to create cube-specific 
`.geojson`-files that can serve as inputs for the further processing steps.
This script reads in locations from `event_coordinates.csv`. 
For each location a config is created in the `configs` subfolder.




