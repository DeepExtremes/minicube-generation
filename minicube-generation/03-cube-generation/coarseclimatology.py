from typing import List
import numpy as np
import xarray as xr

SURF_REC_MIN = 0 # (7272.72 * 0.0000275) - 0.2
SURF_REC_MAX = 1.6 # (65455 * 0.0000275) - 0.2
BLUE_MAX = 0.375025 # (20910 * 0.0000275) - 0.2

L57_CLOUD_BIT = 2
L57_CLOUD_SHADOW_BIT = 4
L57_CLOUD_BITS = [L57_CLOUD_BIT, L57_CLOUD_SHADOW_BIT]

L8_CLOUD_SHADOW_BIT = 8
L8_HIGH_CLOUD_CONFIDENCE_BIT = 96
L8_HIGH_CIRRUS_CONFIDENCE_BIT = 384
L8_CLOUD_BITS = [L8_CLOUD_SHADOW_BIT,
                 L8_HIGH_CLOUD_CONFIDENCE_BIT,
                 L8_HIGH_CIRRUS_CONFIDENCE_BIT]
NIR = 'B05'
RED = 'B04'


def mask_data(ds: xr.Dataset, cloud_bits: List[int], quality_band_name: str):
    cloud_qa = ds[quality_band_name]

    mask = xr.where(cloud_qa & cloud_bits[0] == 0, True, False)
    for cloud_bit in cloud_bits:
        mask = xr.where(cloud_qa & cloud_bit == 0, mask, False)

    sr_var_names = ['B02', 'B03', 'B04', 'B05']
    sr_vars = [cloud_qa]
    for sr_var_name in sr_var_names:
        sr_var = ds[sr_var_name]
        mask = xr.where(sr_var > SURF_REC_MIN, mask, False)
        if sr_var_name == 'B02':
            mask = xr.where(sr_var < BLUE_MAX, mask, False)
        else:
            mask = xr.where(sr_var < SURF_REC_MAX, mask, False)
        sr_vars.append(sr_var)
    sr_vars.append(mask.rename('mask'))
    merged = xr.merge(sr_vars, join="inner")
    return merged


def _ndvi(ds: xr.Dataset) -> xr.Dataset:
    ndvi_array = (ds[NIR] - ds[RED]) / (ds[NIR] + ds[RED])
    ndvi_array = xr.where(ds.mask, ndvi_array, np.nan)
    return ndvi_array.to_dataset(name='ndvi')


def _harmonize_with_l8(ds: xr.Dataset) -> xr.Dataset:
    ndvi = (ds.ndvi + 0.015) / 0.988
    return ndvi.to_dataset()


def _clear(ds: xr. Dataset) -> xr.DataArray:
    ndvi_cleared = ds.ndvi.where(ds.ndvi >= 0)
    return ndvi_cleared.where(ndvi_cleared <= 1)


def create_climatology(l5ds: xr.Dataset, l7ds: xr.Dataset, l8ds: xr.Dataset):
    masked_l5ds = mask_data(l5ds, L57_CLOUD_BITS, 'SR_CLOUD_QA')
    masked_l7ds = mask_data(l7ds, L57_CLOUD_BITS, 'SR_CLOUD_QA')
    masked_l57ds = xr.concat([masked_l5ds, masked_l7ds], dim='time')
    masked_l8ds = mask_data(l8ds, L8_CLOUD_BITS, 'BQA')
    ndvi57 = _ndvi(masked_l57ds)
    ndvi8 = _ndvi(masked_l8ds)
    harm_ndvi_57 = _harmonize_with_l8(ndvi57)
    ndvi = xr.concat([harm_ndvi_57, ndvi8], dim='time')
    ndvi_cleared = _clear(ndvi)
    ndvi_grouped = ndvi_cleared.groupby('time.month')
    mean = ndvi_grouped.mean().rename('ndvi_mean')
    std = ndvi_grouped.std().rename('ndvi_std')
    return xr.merge([mean, std])
