from datetime import datetime
import fsspec
import geopandas as gpd
import glob
import json
import math
import numpy as np
from pyproj import Proj
from shapely.geometry import Point
from typing import List
from typing import Optional
import os
import xarray as xr

from xcube.core.store import new_data_store

_SPATIAL_RES = 20
_HALF_IMAGE_SIZE = 64 * _SPATIAL_RES
_ID_TEMPLATE = "mc_{lon}_{lat}_{version}_{date}_{count}"
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


def get_available_components():
    config_paths = glob.glob('configs/*.json')
    available_components = []
    for config_path in config_paths:
        available_components.append(config_path[8:-5])
    return available_components


def open_config(path: str, update: bool) -> dict:
    config_file = open(path)
    config = json.load(config_file)
    if 'update' in config:
        if update:
            return config['update']
        return config['base']
    return config


def _get_dem_file_path(lon: float, lat: float) -> str:
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


def _get_dem_file_paths(tc:dict) -> List[str]:
    lower_left_dem_file_path = _get_dem_file_path(
        tc['properties']['metadata']['geospatial_lon_min'],
        tc['properties']['metadata']['geospatial_lat_min']
    )
    lower_right_dem_file_path = _get_dem_file_path(
        tc['properties']['metadata']['geospatial_lon_max'],
        tc['properties']['metadata']['geospatial_lat_min']
    )
    upper_left_dem_file_path = _get_dem_file_path(
        tc['properties']['metadata']['geospatial_lon_min'],
        tc['properties']['metadata']['geospatial_lat_max']
    )
    upper_right_dem_file_path = _get_dem_file_path(
        tc['properties']['metadata']['geospatial_lon_max'],
        tc['properties']['metadata']['geospatial_lat_max']
    )
    dem_files = [lower_left_dem_file_path]
    if lower_left_dem_file_path != lower_right_dem_file_path:
        dem_files.append(lower_right_dem_file_path)
    if lower_left_dem_file_path != upper_left_dem_file_path:
        dem_files.append(upper_left_dem_file_path)
    if upper_left_dem_file_path != upper_right_dem_file_path and \
            lower_right_dem_file_path != upper_right_dem_file_path:
        dem_files.append(upper_right_dem_file_path)
    return dem_files


def fill_config_with_missing_values(
        tc: dict, center_lon: float, center_lat: float
) -> dict:
    dem_file_paths = _get_dem_file_paths(tc)
    for variable in tc['properties']['variables']:
        if variable['name'] == 'cop_dem':
            variable['sources'][0]['processing_steps'][0] = \
                f'Read {dem_file_paths[0]}'
            if len(dem_file_paths) > 1:
                variable['sources'][0]['processing_steps'][1] = \
                    f'Merge with {" ".join(dem_file_paths[1:])}'
        break
    no_ndvi_climatology = False
    for source in tc['properties']['sources']:
        if source['name'] == 'Copernicus DEM 30m':
            current_key = list(source['datasets'].keys())[0]
            dem_datasets_dict_save = source['datasets'][current_key]
            dem_datasets = {}
            for dem_file_path in dem_file_paths:
                dem_datasets[dem_file_path] = dem_datasets_dict_save
            source['datasets'] = dem_datasets
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
    return tc


def get_readable(value: float):
    return "{:.2f}".format(value)


def get_data_id(center_lon_readable: str,
                center_lat_readable: str,
                version: str,
                count: int = 0):
    return _ID_TEMPLATE.format(
        lon=center_lon_readable,
        lat=center_lat_readable,
        version=version,
        date=datetime.now().strftime('%Y%m%d'),
        count=count
    )


def _get_center_from_spatial_bbox(utm_proj: Proj, spatial_bbox: List[float]):
    xmin, ymin, xmax, ymax = spatial_bbox
    x = xmin + _HALF_IMAGE_SIZE
    y = ymin + _HALF_IMAGE_SIZE
    return utm_proj(x, y, inverse=True)


def _get_geospatial_bbox_from_spatial_bbox(utm_proj: Proj,
                                           spatial_bbox: List[float]):
    xmin, ymin, xmax, ymax = spatial_bbox
    lons, lats = utm_proj(
        (xmin, xmin, xmax, xmax, xmin),
        (ymin, ymax, ymax, ymin, ymin),
        inverse=True
    )
    return np.swapaxes(np.asarray((lons, lats)), 0, 1).tolist()


def create_update_config(mc: xr.Dataset, mc_path: str,
                         components_to_update: List[str]):
    update_config = open_config('update.geojson', update=True)
    mc_configuration_versions = \
        mc.attrs.get('metadata', {}).get('configuration_versions', {})
    update_config['properties'] = mc.attrs
    any_changes = False
    for component in components_to_update:
        mc_component_version = mc_configuration_versions.get(component, '-1')
        component_config = open_config(f'configs/{component}.json', update=True)
        current_component_version = \
            component_config.get('properties', {}).get('metadata', {}).\
                get('configuration_versions', {}).get(component)
        if mc_component_version == current_component_version:
            continue
        update_config = merge_configs(component_config, update_config)
        any_changes = True
    if not any_changes:
        return
    crs = update_config['properties']['spatial_ref']
    utm_proj = Proj(crs)
    spatial_bbox = [
        float(coord) for coord in update_config['properties']['spatial_bbox']
    ]
    center_lon = update_config.get('properties').get('metadata').\
        get('geospatial_center_lon')
    center_lat = update_config.get('properties').get('metadata').\
        get('geospatial_center_lat')
    if center_lon is None or center_lat is None:
        center_lon, center_lat = _get_center_from_spatial_bbox(utm_proj,
                                                               spatial_bbox)
        update_config['properties']['metadata']['geospatial_center_lon'] = \
            center_lon
        update_config['properties']['metadata']['geospatial_center_lat'] = \
            center_lat
    update_config = fill_config_with_missing_values(
        update_config, center_lon, center_lat
    )
    geospatial_bbox = _get_geospatial_bbox_from_spatial_bbox(utm_proj,
                                                             spatial_bbox)
    update_config['geometry']['coordinates'][0] = geospatial_bbox
    update_config["properties"]["base_minicube"] = mc_path
    version ='unknown'
    with open('../version.py', 'r') as v:
        version = v.read().split('=')[1]
    center_lon_readable = get_readable(center_lon)
    center_lat_readable = get_readable(center_lat)
    count = 0
    data_id = get_data_id(
        center_lon_readable, center_lat_readable, version, count
    )
    while data_id in mc_path:
        count += 1
        data_id = get_data_id(
            center_lon_readable, center_lat_readable, version, count
        )
    update_config["properties"]["data_id"] = data_id
    if not update_config.get('properties').get('location_id'):
        update_config["properties"]["location_id"] = \
            f'{center_lon_readable}_{center_lat_readable}'
    base_fs = get_fs()
    with base_fs.open(
            f'deepextremes-minicubes/configs/update/'
            f'{version}/{data_id}.geojson', 'wb') as mc_json:
        mc_json.write(json.dumps(update_config).encode('utf-8'))


def get_store(bucket: str):
    s3_key = os.environ["S3_USER_STORAGE_KEY"]
    s3_secret = os.environ["S3_USER_STORAGE_SECRET"]
    return new_data_store(
        "s3",
        root=bucket,
        storage_options=dict(
            anon=False,
            key=s3_key,
            secret=s3_secret
        )
    )


def get_fs():
    s3_key = os.environ["S3_USER_STORAGE_KEY"]
    s3_secret = os.environ["S3_USER_STORAGE_SECRET"]
    storage_options = dict(
        anon=False,
        key=s3_key,
        secret=s3_secret
    )
    return fsspec.filesystem('s3', **storage_options)


def merge_configs(config_1: dict, config_2: dict) -> dict:
    merged_config = config_1.copy()

    for key, value in config_2.items():
        if key in merged_config:
            current_value = merged_config[key]
            if isinstance(current_value, list) and isinstance(value, list):
                if len(current_value) > 0 and \
                        isinstance(current_value[0], dict) \
                        and len(value) > 0 and isinstance(value[0], dict):
                    out_list = []
                    for sub_config_1 in current_value:
                        for sub_config_2 in value:
                            if sub_config_1['name'] == sub_config_2['name']:
                                current_value.remove(sub_config_1)
                                value.remove(sub_config_2)
                                out_list.append(
                                    merge_configs(sub_config_1, sub_config_2)
                                )
                                break
                    out_list += current_value
                    out_list += value
                    merged_config[key] = out_list
                else:
                    merged_config[key] = current_value + value
            elif isinstance(current_value, dict) and isinstance(value, dict):
                merged_config[key] = merge_configs(current_value, value)
        else:
            merged_config[key] = value

    return merged_config
