from datetime import timedelta
import math
import pandas as pd
import subprocess
import time

from xcube.core.store import new_data_store

from era5_utils import get_list_of_combinations

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
MAX_NUM_RUNS = 95
MAX_NUM_PARALLEL_PROCESSES = 12


running_processes = {}
safe_datasets = []


def _get_lat_lon_from_era5_file_path(era5_file_path: str) -> (int, int, str):
    _, lats, lons, _ = era5_file_path.split('_')
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


def _get_era5_file_name(lon: int, lat: int, var_name: str) -> str:
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
    return f'era5land_{var_name}_{lat_str}_{lon_str}_v{version}.zarr'


def _start_next_processes(num_running_processes: int):
    var_stores = {}
    for var_name in VAR_NAMES.keys():
        var_stores[var_name] = new_data_store(
            "s3",
            root=f"deepextremes/era5land/{var_name}/"
        )
    for lon_lat_combination in get_list_of_combinations():
        lon, lat = lon_lat_combination
        for var_name in VAR_NAMES.keys():
            era5_file_name = _get_era5_file_name(lon, lat, var_name)
            if era5_file_name in safe_datasets:
                continue
            if var_stores[var_name].has_data(era5_file_name):
                # check whether there is a process running
                running_process = running_processes.get(era5_file_name)
                if running_process is not None:
                    print(f'{era5_file_name} is currently being processed')
                    continue
                # check whether dataset holds all time steps
                var_ds = var_stores[var_name].open_data(era5_file_name)
                last_datetime = pd.to_datetime(str(var_ds.time[-1].values))
                if last_datetime == END_TIMESTAMP:
                    # dataset is good
                    print(f'Checked {era5_file_name}, all fine')
                    safe_datasets.append(era5_file_name)
                    continue
                # dataset is incomplete, need to fill
                formatted_last_datetime = last_datetime.strftime('%Y-%m-%d')
                print(f'Last timestamp of {era5_file_name} was '
                      f'{formatted_last_datetime}, will add missing time steps')
                new_start_date = last_datetime + ONE_DAY
                formatted_new_start_date = new_start_date.strftime('%Y-%m-%d')
                _generate_era5_cube(lon, lat, var_name, formatted_new_start_date)
            else:
                # dataset is missing, create
                _generate_era5_cube(lon, lat, var_name)
            num_running_processes += 1
            if num_running_processes >= MAX_NUM_PARALLEL_PROCESSES:
                return


def _generate_era5_cube(lon: int, lat: int, var_name: str,
                        formatted_new_start_date: str = None):
    var_long_name = VAR_NAMES[var_name]
    era5_file_name = _get_era5_file_name(lon, lat, var_name)
    print(f'Starting processing of {era5_file_name}')
    running_processes[era5_file_name] = \
        subprocess.Popen(["srun", "--time=48:00:00", "--partition=long-serial",
                          "python", "generate-era5-cube.py",
                          f'{lon}', f'{lon + 10}',
                          f'{lat}', f'{lat + 10}',
                          f'{var_long_name}', f'{var_name}',
                          f'{formatted_new_start_date}'])


def _check_running_processes():
    processes_to_remove = []
    for process_name, process in running_processes.items():
        if process.poll() is not None:
            print(f'Process {process_name} has code {process.returncode}, will end')
            processes_to_remove.append(process_name)
        else:
            print(f'Process {process_name} has no return code, is still running')
    for process in processes_to_remove:
        running_processes.pop(process)
    return len(running_processes.items())


def generate_era5_cubes():
    num_runs = 0
    while(num_runs < MAX_NUM_RUNS):
        num_running_processes =  _check_running_processes()
        if num_running_processes < MAX_NUM_PARALLEL_PROCESSES:
            _start_next_processes(num_running_processes)
        time.sleep(2700)
        num_runs += 1


if __name__ == "__main__":
    generate_era5_cubes()
