from datetime import datetime
import copy
import geopandas as gpd
import glob
import json
import math
import numpy as np
import os
import pandas as pd
from pyproj import CRS
from pyproj import Proj
from shapely.geometry import Point
import sys
from typing import Optional


_ID_TEMPLATE = "mc_{lon}_{lat}_{version}"
_TITLE_TEMPLATE = "Minicube at {lon} {lat}"
_DESCRIPTION_TEMPLATE = "The minicube covering the area around {lon} and {lat}"
_SPATIAL_RES = 20
_HALF_IMAGE_SIZE = 64 * _SPATIAL_RES
_ERA5_VARIABLE_NAMES = [
    "e_max", "e_min", "e_mean", "pev_max", "pev_min", "pev_mean", "slhf_max",
    "slhf_min", "slhf_mean", "sp_max", "sp_min", "sp_mean", "sshf_max",
    "sshf_min", "sshf_mean", "ssr_max", "ssr_min", "ssr_mean", "t2m_max",
    "t2m_min", "t2m_mean", "tp_max", "tp_min", "tp_mean"
]
_ERA5_VARIABLE_MAP = {
    '2m Temperature': 't2m',
    'Potential Evaporation': 'pev',
    'Surface Latent Heat Flux': 'slhf',
    'Surface Net Solar Radiation': 'ssr',
    'Surface Pressure': 'sp',
    'Surface Sensible Heat Flux': 'sshf',
    'Total Evaporation': 'e',
    'Total Precipitation': 'tp'
}
_REGIONS = gpd.read_file(
    'https://explorer.digitalearth.africa/api/regions/ndvi_climatology_ls'
)
_SHORT_MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']


def _get_crs(lon: float, lat: float) -> str:
    utm_zone = int(math.floor(lon + 180) / 6)
    hemisphere = "south" if lat < 0 else "north"
    crs = CRS.from_string(f'+proj=utm +zone={utm_zone} +{hemisphere}')
    epsg_code = crs.to_epsg()
    return f'EPSG:{epsg_code}'


def _get_dem_file_path(lon: int, lat: int) -> str:
    if lon < 0:
        lon_str = f'W{int(abs(lon - 1)):03}'
    else:
        lon_str = f'E{int(abs(lon)):03}'
    if lat < 0:
        lat_str = f'S{int(abs(lat - 1)):02}'
    else:
        lat_str = f'N{int(abs(lat)):02}'
    return f'Copernicus_DSM_COG_10_{lat_str}_00_{lon_str}_00_DEM/' \
           f'Copernicus_DSM_COG_10_{lat_str}_00_{lon_str}_00_DEM.tif'


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
    return f'era5land_{var_name}_{lat_str}_{lon_str}_v1.zarr'


def _get_deafrica_ndvi_file_path(lon: float, lat: float, var_name: str) \
        -> Optional[str]:
    point = Point(lon, lat)
    containing_regions = _REGIONS[_REGIONS.geometry.contains(point)]
    if len(containing_regions) == 0:
        return
    region_code = containing_regions.iloc[0].region_code
    return f'deafrica-services/ndvi_climatology_ls/1-0-0/' \
           f'{region_code[0:4]}/{region_code[4:]}/1984--P37Y/' \
           f'ndvi_climatology_ls_{region_code[0:4]}{region_code[4:]}_' \
           f'1984--P37Y_{var_name}.tif'


def generate_minicube_configs(location_file: str):
    minicube_locations = pd.read_csv(location_file, delimiter="\t")
    version ='unknown'
    with open('../version.py', 'r') as v:
        version = v.read().split('=')[1]
    if not os.path.exists(f'../configs/{version}'):
        os.makedirs(f'../configs/{version}/')
    with open('../cube.geojson', 'r') as template:
        t = json.load(template)
        for minicube_location in minicube_locations.iterrows():
            tc = copy.deepcopy(t)
            center_lon = minicube_location[1].Longitude
            center_lon_readable = "{:.2f}".format(center_lon)
            center_lat = minicube_location[1].Latitude
            center_lat_readable = "{:.2f}".format(center_lat)
            crs = _get_crs(center_lon, center_lat)
            utm_proj = Proj(crs)
            x, y = utm_proj(center_lon, center_lat)
            xmin = x - _HALF_IMAGE_SIZE
            xmax = x + _HALF_IMAGE_SIZE
            ymin = y - _HALF_IMAGE_SIZE
            ymax = y + _HALF_IMAGE_SIZE
            lons, lats = utm_proj(
                (xmin, xmin, xmax, xmax, xmin),
                (ymin, ymax, ymax, ymin, ymin),
                inverse=True
            )
            geospatial_bbox = \
                np.swapaxes(np.asarray((lons, lats)), 0, 1).tolist()
            tc['geometry']['coordinates'][0] = geospatial_bbox
            tc['properties']['title'] = _TITLE_TEMPLATE.\
                format(lon=center_lon_readable, lat=center_lat_readable)
            data_id = _ID_TEMPLATE.format(lon=center_lon_readable,
                                          lat=center_lat_readable,
                                          version=version)
            tc['properties']['data_id'] = data_id
            tc['properties']['description'] = _DESCRIPTION_TEMPLATE.\
                format(lon=center_lon_readable, lat=center_lat_readable)
            tc['properties']['version'] = version
            tc['properties']['spatial_res'] = _SPATIAL_RES
            tc['properties']['spatial_ref'] = crs
            tc['properties']['spatial_bbox'] = [xmin, ymin, xmax, ymax]
            tc['properties']['metadata']['date_modified'] = \
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            tc['properties']['metadata']['event_label'] = \
                minicube_location[1].EventLabel
            tc['properties']['metadata']['event_start_time'] = \
                minicube_location[1].EventStart
            tc['properties']['metadata']['event_end_time'] = \
                minicube_location[1].EventEnd
            tc['properties']['metadata']['geospatial_lon_min'] = min(lons)
            tc['properties']['metadata']['geospatial_lon_max'] = max(lons)
            tc['properties']['metadata']['geospatial_lat_min'] = min(lats)
            tc['properties']['metadata']['geospatial_lat_max'] = max(lats)
            tc['properties']['metadata']['class'] = minicube_location[1].Class
            dem_file_path = _get_dem_file_path(center_lon, center_lat)
            for variable in tc['properties']['variables']:
                if variable['name'] == 'cop_dem':
                    variable['sources'][0]['processing_steps'][0] = \
                        f'Read {dem_file_path}'
                    break
            no_ndvi_climatology = False
            for source in tc['properties']['sources']:
                if source['name'] == 'Copernicus DEM 30m':
                    current_key = list(source['datasets'].keys())[0]
                    dem_datasets_dict_save = source['datasets'][current_key]
                    source['datasets'] = {
                        dem_file_path: dem_datasets_dict_save
                    }
                elif source['name'] == 'NDVI Climatology':
                    for ds_value_dict in source['datasets'].values():
                        for da_key, da_dict in \
                                ds_value_dict['dataarrays'].items():
                            da_path = _get_deafrica_ndvi_file_path(
                                center_lon,
                                center_lat,
                                str(da_key)
                            )
                            if da_path is None:
                                # remove NDVI Climatology
                                tc['properties']['sources'].remove(source)
                                no_ndvi_climatology = True
                                break
                            da_dict['path'] = da_path
                        if no_ndvi_climatology:
                            break
            remove_variables = []
            for variable in tc['properties']['variables']:
                if variable['name'] in _ERA5_VARIABLE_NAMES:
                    var_name_start = variable['name'].split('_')[0]
                    era5_file_path = _get_era5_file_path(center_lon,
                                                         center_lat,
                                                         var_name_start)
                    variable['sources'][0]['processing_steps'][0] = \
                        f'Read {era5_file_path}'
                if variable['name'].startswith('ndvi_climatology') \
                        and no_ndvi_climatology:
                    remove_variables.append(variable)
            for variable in remove_variables:
                tc['properties']['variables'].remove(variable)
            for source in tc['properties']['sources']:
                if source['name'].startswith('Era-5 Land'):
                    current_key = list(source['datasets'].keys())[0]
                    era5_datasets_dict_save = source['datasets'][current_key]
                    long_var_name = source['name'].split('Era-5 Land ')[1]
                    var_name = _ERA5_VARIABLE_MAP[long_var_name]
                    source_era5_file_path = _get_era5_file_path(center_lon,
                                                                center_lat,
                                                                var_name)
                    source['datasets'] = {
                        source_era5_file_path: era5_datasets_dict_save
                    }
            with open(f'../configs/{version}/{data_id}.geojson',
                      'w+') as mc_json:
                json.dump(tc, mc_json, indent=4)


def _merge_configs(dict1, dict2):
    merged_dict = dict1.copy()

    for key, value in dict2.items():
        if key in merged_dict:
            current_value = merged_dict[key]
            if isinstance(current_value, list) and isinstance(value, list):
                if len(current_value) > 0 and isinstance(current_value[0], dict) and len(value) > 0 and isinstance(value[0], dict):
                    out_list = []
                    for sub_dict_1 in current_value:
                        for sub_dict_2 in value:
                            if sub_dict_1['name'] == sub_dict_2['name']:
                                current_value.remove(sub_dict_1)
                                value.remove(sub_dict_2)
                                out_list.append(_merge_configs(sub_dict_1, sub_dict_2))
                                break
                    out_list += current_value
                    out_list += value
                    merged_dict[key] = out_list
                else:
                    merged_dict[key] = current_value + value
            elif isinstance(current_value, dict) and isinstance(value, dict):
                merged_dict[key] = _merge_configs(current_value, value)
        else:
            merged_dict[key] = value

    return merged_dict


if __name__ == "__main__":
    if len(sys.argv) == 2:
        location_file = sys.argv[1]
    elif len(sys.argv) == 1:
        version = 'unknown'
        with open('../01-location-extraction/locationversions.py', 'r') as v:
            version = v.read().split('=')[1]
        date = datetime.now().strftime('%Y-%m-%d')
        filename = f'../minicube_locations_v{version}_{date}_*.csv'
        print(f'Looking for files matching pattern {filename}')
        location_files = glob.glob(filename)
        if len(location_files) == 0:
            raise ValueError('Could not determine location file, '
                             'please state one')
        location_file = location_files[0]
        print(f'No location file given, will pick {location_file}')
    else:
        raise ValueError('Too many arguments given')
    generate_minicube_configs(location_file)
