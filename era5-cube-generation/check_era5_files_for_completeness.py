import pandas as pd
import subprocess
from datetime import timedelta

from xcube.core.store import new_data_store

VAR_NAMES = {
    't2m': '2m_temperature',
    'pev': 'potential_evaporation',
    'slhf': 'surface_latent_heat_flux',
    'ssr': 'surface_net_solar_radiation',
    'sp': 'surface_pressure',
    'sshf': 'surface_sensible_heat_flux',
    'e': 'total_evaporation',
    'tp': 'total_precipitation'
}
END_TIMESTAMP = pd.Timestamp(2022, 10, 31)
ONE_DAY = timedelta(days=1)


def _get_lat_lon_from_era5_file_path(era5_file_path: str) -> (int, int):
    _, _, lats, lons, _ = era5_file_path.split('_')
    if lons[0] == 'W':
        lon = -1 * int(lons[1:])
    elif lons[0] == 'E':
        lon = int(lons[1:])
    else:
        raise ValueError(f'Could not determine longitude from {era5_file_path}')
    if lats[0] == 'S':
        lat = -1 * int(lats[1:])
    elif lats[0] == 'N':
        lat = int(lats[1:])
    else:
        raise ValueError(f'Could not determine latitude from {era5_file_path}')
    return lat, lon


def _check_for_completeness():
    for var_name in VAR_NAMES.keys():
        var_store = new_data_store(
            "s3",
            root=f"deepextremes/era5land/{var_name}/"
        )
        for var_ds_id in var_store.get_data_ids():
            var_ds = var_store.open_data(var_ds_id)
            last_datetime = pd.to_datetime(str(var_ds.time[-1].values))
            if last_datetime == END_TIMESTAMP:
                print(f'Checked {var_ds_id}, all fine')
                continue
            formatted_last_datetime = last_datetime.strftime('%Y-%m-%d')
            print(f'Last timestamp of {var_ds_id} was '
                  f'{formatted_last_datetime}, will add missing time steps')
            new_start_date = last_datetime + ONE_DAY
            formatted_new_start_date = new_start_date.strftime('%Y-%m-%d')
            lat, lon = _get_lat_lon_from_era5_file_path(var_ds_id)
            var_long_name = VAR_NAMES[var_name]
            # print(f'cmd: python generate-era5-cube.py {lon} {lon + 10} '
            #       f'{lat} {lat + 10} {var_long_name} {var_name}',
            #       f'{formatted_new_start_date}')
            subprocess.Popen(['python', 'generate-era5-cube.py',
                              f'{lon}', f'{lon + 10}',
                              f'{lat}', f'{lat + 10}',
                              f'{var_long_name}', f'{var_name}',
                              f'{formatted_new_start_date}'])


if __name__ == "__main__":
    _check_for_completeness()
