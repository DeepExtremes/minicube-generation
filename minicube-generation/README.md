# DeepExtremes Minicube Generation

Contains the instructions on how to create minicubes for the DeepExtremes 
project.

`cube.geojson` contains a template for creating minicubes.
Move to `01-location-extraction` and execute `extract-locations.py`.
This file expects four additional coordinates: minimum longitude, 
maximum longitude, minimum latitude and maximum longitude, so run it like, 
e.g., `extract-locations.py 25 35 50 60`. 
It will expect `.zarr`-files in the `Locations` folder at project level.
There is a file `locationversions.py` here that will keep track of the way
the locations are extracted.
The output is a `.csv`-file in the `minicube-generation`-folder, 
and it will carry the date, the bounding box, and the version in its name. 

Next, move to `01-config-generation` and run `generate-config.py` to create 
cube-specific `.geojson`-files that can serve as inputs for the cube generation.
You may pass to this script explicitly a location file created in the previous
step, if you dont't do so, the script will look for a valid file itself.
For each location a config is created in the `configs` subfolder at 
`minicube-generation` level.

Next, move to `03-cube-generation` and run `generate_cubes.py` to actually 
generate minicubes.
This script expects five arguments: The first is either a single minicube 
configuration geojson file, or a folder containing several of these files.
Folders must end with an`*`. The second parameter is the sentinelhub client id,
the third one the sentinelhub client secret, 
the fourth one the aws access key id, the fifht one the aws secret access key.
The miincubes are written directly to the `deepextremes` bucket on AWS.
