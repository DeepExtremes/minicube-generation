import math
import subprocess
import sys

from datetime import datetime

_START = datetime(2016, 1, 1)
_NEW_START = datetime(2017, 12, 31)
_END = datetime(2022, 10, 8)
_VARIABLE_NAMES = {
    '2m_temperature': 't2m',
    'potential_evaporation': 'pev',
    'surface_latent_heat_flux': 'slhf',
    'surface_net_solar_radiation': 'ssr',
    'surface_pressure': 'sp',
    'surface_sensible_heat_flux': 'sshf',
    'total_evaporation': 'e',
    'total_precipitation': 'tp'
}


def generate_era5_cubes(min_lon: float, max_lon:float,
                        min_lat: float, max_lat: float):
    min_lon = int(math.floor(min_lon))
    max_lon = int(math.ceil(max_lon))
    min_lat = int(math.floor(min_lat))
    max_lat = int(math.ceil(max_lat))
    for lon in range(min_lon, max_lon, 10):
        for lat in range(min_lat, max_lat, 10):
            for var_long_name, var_name in _VARIABLE_NAMES.items():
                subprocess.Popen(['python', 'generate-era5-cube.py',
                                  f'{lon}', f'{lon +10}',
                                  f'{lat}', f'{lat + 10}',
                                  f'{var_long_name}', f'{var_name}'])


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise ValueError('Expected coordinates '
                         'MIN_LON, MAX_LON, MIN_LAT, MAX_LAT')
    else:
        coords = sys.argv[1:]
        generate_era5_cubes(float(coords[0]),
                            float(coords[1]),
                            float(coords[2]),
                            float(coords[3]))
