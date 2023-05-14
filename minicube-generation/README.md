# DeepExtremes Minicube Generation

Contains the instructions on how to create minicubes for the DeepExtremes 
project.

## Location Extraction 

This part is by not in use anymore, as locations are not extracted by this 
software anymore but provided by project partners as csv tables.

The following is just for documentation purposes:

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

## Config Generation

This is the part where configs for minicube creation are constructed.
There are two types of configs: base and update. 
Base configs serve to create entirely new minicubes, update configs allow to
update existing minicubes with new or updated components.
All generations are run by cli scripts that are supposed to be run from
`02-config-generation`. 

To create a base config, run `generate-base-configs.py` and pass as single 
parameter the path to the .csv-table containing the locations.
The configs will be written to the `deepextremes-minicubes` bucket under
`deepextremes-minicubes/configs/base/<version>/`.
A base configuration will consist of the following components: `s2_l2_bands`, 
`copernicus_dem`, `de_africa_climatology`, `era5`, `event_arrays`, 
`cci_landcover_map`.

To create an update config, you can either run `generate-update-config` 
or `generate-update-configs`, depending on whether you want to update one single
minicube or multiple ones. In the former case, you need to add the path to the
minicube in a bucket, in the latter case, the prefix on the bucket is required.
In any case you also need to provide a list of the components that need to be
updated.
These components might be: `s2_l2_bands`, 
`copernicus_dem`, `de_africa_climatology`, `era5`, `event_arrays`, 
`cci_landcover_map`, `s2cloudless_cloud_mask`, `sen2cor_cloudmask`, 
`unetmobv2_cloudmask`.
In case the config generation datects that the minicube already has the latest
version of the component, that component will not be updated.
If there are no components to be updated, no config will be written.
The configs will be written to the `deepextremes-minicubes` bucket under
`deepextremes-minicubes/configs/update/<version>/`.

## Cube Generation

This is the actual minicube generation. 
You may execute it by running `generate_cubes.py` from `03-cube-gneration` 
This script expects three arguments: The first is either a single minicube 
configuration geojson file, or a bucket path containing several of these files. 
The second parameter the aws access key id and the third one the aws secret 
access key to access the `deepextremes` bucket holding auxiliary data.
There is a fourth optional parameter that expects an integer value. If you
use this, generation processes will be started every minutes as indicated by
this parameter. If omitted, processes are run consecutively.
The minicubes are written to the `deepextremes-minicubes` bucket on AWS,
registry entries are written to `deepextremes-minicubes/mc_registry_v2.csv`.
In case the configs are update configs, the previous minicubes are deleted and 
their entries removed from the registry.

## Pinging

In `04-pinger`, there is script `pinger.py` that will print an output line 
every ten minutes. Run if needed. 
