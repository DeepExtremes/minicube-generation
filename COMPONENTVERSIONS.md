# ComponentVersioning

## s2_l2_bands
### 1.0
* Initial version with bands "B02", "B03", "B04", "B05", "B06", "B07", "B8A", 
  "SCL"
### 1.1
* Fixed setting of mosaicking_period: Ensure tiles with least cloud coverage 
  were used
### 1.2
* Reverted setting as it was redundant (versions 1.0 to 1.2 all offer least 
  cloud coverage)
### 1.3
* Read all bands for a time step in one request (functional change that results
  in slightly different metadata)
### 1.4 
* Do not read time steps not provided by sentinelhub 
* Set no data areas to nan (are 0.0 in version 1.3)
* Ensure SCL is written as integer data type
  
## era5
### 1.0
* Initial version with bands "e_max", "e_min", "e_mean", "pev_max", "pev_min", 
  "pev_mean", "slhf_max", "slhf_min", "slhf_mean", "sp_max", "sp_min", 
  "sp_mean", "sshf_max", "sshf_min", "sshf_mean", "ssr_max", "ssr_min", 
  "ssr_mean", "t2m_max", "t2m_min", "t2m_mean", "tp_max", "tp_min", "tp_mean"

## cci_landcover_map
### 1.0
* Initial version with band "lccs_class"
### 2.0 
* Top line of data is not empty anymore  
### 2.1
* Changed link to landcover map after renaming of auxiliary data set

## copernicus_dem
### 1.0
* Initial version with band "cop_dem"

## de_africa_climatology
### 1.0 
 * Initial version with bands "mean_jan", "mean_feb", "mean_mar", "mean_apr", 
   "mean_may", "mean_jun", "mean_jul", "mean_aug", "mean_sep", "mean_oct", 
   "mean_nov", "mean_dec", "stddev_jan", "stddev_feb", "stddev_mear", 
   "stddev_apr", "stddev_may", "stddev_jun", "stddev_jul", "stddev_aug", 
   "stddev_sep", "stddev_oct", "stddev_nov", "stddev_dec"
   
   These are only available for locations in Africa.

## event_arrays
### 1.0 
* Initial version with bands "events" and "event_labels"
### 2.0
* Read events correctly
* Save as integers rather than floating point numbers
### 2.1
* Small metadata adjustment (correct data type)

## s2cloudless_cloudmask
### 1.0 
* Initial version with band "CLM"
### 1.1
* Also include band "CLP"
### 1.2
* Read all bands for a time step in one request (functional change that results
  in slightly different metadata)  

## sen2cor_cloudmask
### 1.0 
* Initial version with band "CLD"
### 1.1
* Read all bands for a time step in one request (functional change that results
  in slightly different metadata)  

## unetmobv2_cloudmask
### 1.0 
* Initial version with bands "cloud_clear_prob", "cloud_thick_prob", 
  "cloud_thin_prob", "cloud_shadow_prob"

## earthnet_cloudmask
### 1.0
* Initial version with band "cloudmask_en" and flag coding:
    0 - free_sky
    1 - cloud
    2 - cloud_shadows
    3 - snow
    4 - masked_other_reasons
### 1.1
* Do not process time steps not provided by sentinelhub
* Ensure band is written as integer data type