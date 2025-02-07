## Changes in 1.3.1 (in development)
* Fixed determination of utm zones

## Changes in 1.3
* Adapted location creation to consider events from mixed classes
* Use new registry with new columns `class`, `dominant_class`, 
  `second_dominant_class`, `type`, and `location_source`

## Changes in 1.2.2
* Do not update minicubes created from csv files
* Fixed typo in landcover map configuration

## Changes in 1.2.1
* Changed link to landcover map after renaming of auxiliary data set
* base config versions are suffixed with base

## Changes in 1.2
* Update `create-location-files.py` to support json and csv
* Improved support of outside events
* Cleaned up repository
* Do not read time steps not provided by sentinelhub
* Set no data areas in s2 l2 data to nan
* Introduced script `generate-base-and-update-cubes` to use resources more 
  efficiently
* Added sandbox mechanism 
* Ensure earthnet cloudmask and scl are written as integer data types

## Changes in 1.1
* Changed cube generation process to start processes until maximum is reached
* Use `four_d` parameter for sentinelhub store to decrease number of requests
* Introduced `COMPONONENTVERSIONS.md`
* Version 2.0 of event cubes: Correctly adjust the longitude so that correct
  values are read.
  Also, changed datatype from floating point to integer values
* Version 2.0 of Copernicus DEM: Cases where minicube areas span over multiple
  DEM tiles are considered.
* Version 2.0 of Landcover Map: Top line of data is not empty anymore
* Version 1.0 of Earthnet Cloud Mask with band cloudmask_en and flag coding:
    0 - free_sky
    1 - cloud
    2 - cloud_shadows
    3 - snow
    4 - masked_other_reasons
* Allow specifying base and update configurations  
* Added script `create-location-files.py` to create location files from 
  event tables on s3
* Use new table `mc_registry_v3.csv` with new column `earthnet_cloudmask`  

## Changes in 1.0
* Introduced updating mechanism and completely overhauled config generation.
  There are now two types of configuration files: base and update.
  Base config files serve to create new minicubes, update files to update 
  existing minicubes. Configs are written to the `deepextremes-minicubes` 
  bucket. 
  There is no longer a single central `.geojson`, but depending on what 
  components are required, minicube configurations are created by merging 
  component `.jsons` (given in the `configs`-subfolder) into either a base or
  an update geojson. Update geojsons will hold the path to the minicube which
  they are supposed to update.
* Write data with encoding if provided

## Changes in 0.3.5
* Added pinger script
* Move configuration files to dedicated folder after minicube creation
* `generate_cubes.py` may now run cube generation processes in parallel. 
  When the script is started with an additional parameter `minutes` it will
  start a new process every given minutes. If the parameter is omitted,
  the processes are run consecutively.
* Added check for existing minicubes
* Include also sen2cor and s2cloudless cloud probabilities from sentinelhub

## Changes in 0.3.4
* Removed cloud mask from recipe 

## Changes in 0.3.3
* fixed lc name

## Changes in 0.3.2
* Improved performance of cloud mask processing 
  (using gpu, batch mode and dask)
* Added script to restrict regions to Africa 
  (where NDVI climatology is available)

## Changes in 0.3.1
* Include Columns from Event and Label Cubes to include all event information

## Changes in 0.3
* Include NDVI Climatologies for regions over Africa
* Include maskay cloud mask (no gpu support yet)
* Include event ids, start time and end time in 
* Include Scene Classification Layer and S2 Cloud Mask
* Use better Land Cover Map
* Removed need to set sentinelhub credentials
* Use S2 images with least cloud coverage

## Changes in 0.0.2
* Include Era5 Land data, CCI Land Cover Map, and Copernicus DEM
* Integrated Processing steps for resampling temporally, resampling spatially,
  chunking, variable renaming, creating spatial subsets, and more.

## Changes in 0.0.1

* Initial version with Sentinel-2 variables only
