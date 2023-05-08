# DeepExtremes Minicube Generation

Contains the instructions on how to create minicubes for the DeepExtremes 
project.

`cube.geojson` contains a template for creating minicubes.
Move to `01-location-extraction` and execute `extract-event-locations.py`.
This file expects four additional coordinates: minimum longitude, 
maximum longitude, minimum latitude and maximum longitude, so run it like, 
e.g., `extract-locations.py 20 30 50 60`.
The output is a `.csv`-file in the `minicube-generation`-folder, 
and it will carry the date, the bounding box, and the version in its name. 
This script accesses the event table and the label cube directly to get
information about distinct events.
Alternatively, you may use the script `extract-locations.py`. 
It will expect `.zarr`-files in the `Locations` folder at project level.
There is a file `locationversions.py` here that will keep track of the way
the locations are extracted.

Next, move to `02-config-generation` and run `generate-config.py` to create 
cube-specific `.geojson`-files that can serve as inputs for the cube generation.
You may pass to this script explicitly a location file created in the previous
step, if you dont't do so, the script will look for a valid file itself.
For each location a config is created in a version-specific subfolder under 
`minicube-generation/configs`.

Next, move to `03-cube-generation` and run `generate_cubes.py` to actually 
generate minicubes.
This script expects three arguments: The first is either a single minicube 
configuration geojson file, or a folder containing several of these files.
Folders must end with an`*`. The second parameter the aws access key id  
and the third one the aws secret access key to access the `deepextremes` bucket
holding auxiliary data.
There is a fourth optional parameter that expects an integer value. If you
use this, generation processes will be started every minutes as indicated by
this parameter. If omitted, processes are run consecutively.
The minicubes are written to the `deepextremes-minicubes` bucket on AWS.

In `04-pinger`, there is script `pinger.py` that will print an output line 
every ten minutes. Run if needed. 
