## Changes in 1.0.1
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
