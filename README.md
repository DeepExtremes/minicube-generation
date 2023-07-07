# DeepExtremes Minicube Generation

Contains the instructions on how to create minicubes for the DeepExtremes 
project.
Note that most of these scripts require you to have environment variables 
`S3_USER_STORAGE_KEY` and `S3_USER_STORAGE_SECRET` set.

## Location Extraction 

Move to `minicube-generation/01-location-extraction` and execute 
`create-locations.py`. 
This will create files `json_events.csv` and `json_non_events.csv` that may
serve as input for the configuration generation.

The following is deprecated:

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
`cci_landcover_map`, and `earthnet_cloudmask`.

To create an update config, you can either run `generate-update-config` 
or `generate-update-configs`, depending on whether you want to update one single
minicube or multiple ones. In the former case, you need to add the path to the
minicube in a bucket, in the latter case, the prefix on the bucket is required.
In any case you also need to provide a list of the components that need to be
updated.
These components might be: `s2_l2_bands`, 
`copernicus_dem`, `de_africa_climatology`, `era5`, `event_arrays`, 
`cci_landcover_map`, `s2cloudless_cloud_mask`, `sen2cor_cloudmask`, 
`unetmobv2_cloudmask`, or `earthnet_cloudmask`.
In case the config generation detects that the minicube already has the latest
version of the component, that component will not be updated.
If there are no components to be updated, no config will be written.
The configs will be written to the `deepextremes-minicubes` bucket under
`deepextremes-minicubes/configs/update/<version>/`.

## Cube Generation

This is the actual minicube generation, which is supposed to be started from . 
The simplest way to run this is to run the script 
`generate-base-and-update-configs.py`  from `03-cube-generation`. 
It receives four to five arguments: 
* a list of base config versions (e.g., 1.1.p,1.2.p)
* the number of base processes that may be run simultaneously
* a list of update config versions (e.g., 1.1,1.2)
* the number of update processes that may be run simultaneously
* optionally, the word sandbox to indicate not to use the real registry
The script will then run the `generate_cubes.py` script defined below twice, 
once for base and once for update configurations; in case one type of 
configurations is not available anymore, the remaining processes will be shifted 
to the other type. 

Otherwise, you may execute it by running `generate_cubes.py`. 
This script expects two arguments: The first is either a single minicube 
configuration geojson file, or a bucket path containing several of these files. 
The second parameter expects an integer value. As many processes as indicated by 
this parameter will be run in parallel, the processes are started individually
every sixty seconds. There is also a third, optional parameter: If you add the
word sandbox, a sandbox registry will be used. 
The minicubes are written to the `deepextremes-minicubes` bucket on AWS,
registry entries are written to `deepextremes-minicubes/mc_registry_v3.csv`.

In case the configs are update configs, the previous minicubes are deleted and 
their entries removed from the registry.

## Pinging

In `04-pinger`, there is script `pinger.py` that will print an output line 
every ten minutes. Run if needed. 
