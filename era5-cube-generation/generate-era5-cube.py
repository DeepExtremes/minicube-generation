import math
import sys
import xarray as xr

from datetime import datetime
from datetime import timedelta

from xcube.core.store import new_data_store

_START = datetime(2016, 1, 1)
_NEW_START = datetime(2016, 1, 1).strftime('%Y-%m-%d')
_END = datetime(2022, 10, 31)

_SOURCE_STORE = new_data_store('cds')


def _get_era5_file_path(lon: int, lat: int, var_name: str) -> str:
    lon_new = int(math.floor(lon / 10) * 10)
    if lon_new < 0:
        lon_str = f'W{int(abs(lon_new)):03}'
    else:
        lon_str = f'E{int(abs(lon_new)):03}'
    lat_new = int(math.floor(lat / 10) * 10)
    if lat_new < 0:
        lat_str = f'S{int(abs(lat_new)):02}'
    else:
        lat_str = f'N{int(abs(lat_new)):02}'
    version =''
    with open('version.py', 'r') as v:
        version = v.read()
    return f's3://deepextremes/era5land/{var_name}/' \
           f'era5land_{var_name}_{lat_str}_{lon_str}_v{version}.zarr'


def _build_era5_cube(min_lon: int, max_lon: int,
                     min_lat: int, max_lat: int,
                     var_long_name: str, var_name: str,
                     new_start_time: str):
    file_path = _get_era5_file_path(min_lon, min_lat, var_name)
    start = datetime.strptime(new_start_time, '%Y-%m-%d')
    print(f'Building cube {file_path}')
    while start < _END:
        stop = start + timedelta(days=7)
        while stop.month != start.month:
            stop -= timedelta(days=1)
        start_str = start.strftime('%Y-%m-%d')
        stop_str = stop.strftime('%Y-%m-%d')
        print(f'Requesting time step from {start_str} to {stop_str} '
              f'for {var_name} between lon {min_lon} and {max_lon} '
              f'and lat {min_lat} and {max_lat}')
        era5 = _SOURCE_STORE.open_data(
            'reanalysis-era5-land',
            variable_names=[var_long_name],
            bbox=[min_lon, min_lat, max_lon, max_lat],
            spatial_res=0.1,
            time_range=[start_str, stop_str]
        )
        resampler = era5.resample(skipna=True,
                                  time='1D')
        era5_max = resampler.max()
        era5_min = resampler.min()
        era5_mean = resampler.mean()
        resamplers = [('max', era5_max),
                      ('min', era5_min),
                      ('mean', era5_mean)
                     ]
        all_rs_ds = []
        for resampler_str, resampled_ds in resamplers:
            for var in era5.data_vars:
                resampled_ds = resampled_ds.assign(
                    {f'{var}_{resampler_str}': resampled_ds[var]}
                )
                resampled_ds = resampled_ds.drop_vars(var)
            all_rs_ds.append(resampled_ds)
        era5_merged = xr.merge(all_rs_ds)
        if start == _START:
            era5_merged.to_zarr(file_path, mode="w", consolidated=True)
        else:
            output_ds = xr.open_zarr(file_path)
            ds_new_only = era5_merged.where(
                era5_merged['time'] > output_ds['time'][-1],
                drop=True)
            ds_new_only.to_zarr(file_path, append_dim='time', consolidated=True)
        start = stop + timedelta(1)
    print(f'Building of {file_path} successfully finished')


if __name__ == "__main__":
    if len(sys.argv) < 7 or len(sys.argv) > 8:
        raise ValueError('Expected coordinates '
                         'MIN_LON, MAX_LON, MIN_LAT, MAX_LAT, '
                         'var_long_name, var_short_name,'
                         'optionally start_date in format yyyy-mm-dd')
    else:
        coords = sys.argv[1:]
        _build_era5_cube(int(coords[0]),
                         int(coords[1]),
                         int(coords[2]),
                         int(coords[3]),
                         str(coords[4]),
                         str(coords[5]),
                         str(coords[6] if len(coords) == 7 else _NEW_START)
                         )
