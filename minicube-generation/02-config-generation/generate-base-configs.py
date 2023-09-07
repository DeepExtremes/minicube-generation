from datetime import datetime
import copy
import json
import math
import numpy as np
import pandas as pd
from pyproj import CRS
from pyproj import Proj
import sys

from configgencreation import fill_config_with_missing_values
from configgencreation import get_data_id
from configgencreation import get_fs
from configgencreation import get_readable
from configgencreation import merge_configs
from configgencreation import open_config


_BASE_COMPONENTS = [
    's2_l2_bands'
]
_ID_TEMPLATE = "mc_{lon}_{lat}_{version}_{date}"
_TITLE_TEMPLATE = "Minicube at {lon} {lat}"
_DESCRIPTION_TEMPLATE = "The minicube covering the area around {lon} and {lat}"
_SPATIAL_RES = 20
_HALF_IMAGE_SIZE = 64 * _SPATIAL_RES


def _create_base_config_template():
    base_config = open_config('base.geojson', update=False)
    for component in _BASE_COMPONENTS:
        component_config = open_config(f'configs/{component}.json', update=False)
        base_config = merge_configs(base_config, component_config)
    return base_config


def _get_crs(lon: float, lat: float) -> str:
    utm_zone = int(math.floor(lon + 180) / 6)
    if utm_zone == 0:
        utm_zone = 60
    hemisphere = "south" if lat < 0 else "north"
    crs = CRS.from_string(f'+proj=utm +zone={utm_zone} +{hemisphere}')
    epsg_code = crs.to_epsg()
    return f'EPSG:{epsg_code}'


def _create_base_config(minicube_location: pd.Series, tc: dict, version: str) -> dict:
    center_lon = minicube_location[1].Longitude
    center_lon_readable = get_readable(center_lon)
    center_lat = minicube_location[1].Latitude
    center_lat_readable = get_readable(center_lat)
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
    geospatial_bbox = np.swapaxes(np.asarray((lons, lats)), 0, 1).tolist()
    tc['geometry']['coordinates'][0] = geospatial_bbox
    tc['properties']['title'] = _TITLE_TEMPLATE. \
        format(lon=center_lon_readable, lat=center_lat_readable)
    data_id = get_data_id(center_lon_readable, center_lat_readable, version)
    tc['properties']['data_id'] = data_id
    tc['properties']['location_id'] = \
        f'{center_lon_readable}_{center_lat_readable}'
    tc['properties']['location_source'] = minicube_location[1].LocationSource
    tc['properties']['description'] = _DESCRIPTION_TEMPLATE. \
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
    tc['properties']['metadata']['geospatial_center_lon'] = center_lon
    tc['properties']['metadata']['geospatial_center_lat'] = center_lat
    tc['properties']['metadata']['class'] = minicube_location[1].Class
    tc['properties']['metadata']['dominant_class'] = \
        minicube_location[1].DominantClass
    tc['properties']['metadata']['second_dominant_class'] = \
        minicube_location[1].SecondDominantClass
    tc = fill_config_with_missing_values(tc, center_lon, center_lat)
    return tc


def _generate_base_configs(location_file: str):
    minicube_locations = pd.read_csv(
        location_file, delimiter="\t", encoding='utf-8',
        converters={
            'EventLabel': pd.eval, 'EventStart': pd.eval, 'EventEnd': pd.eval
        }
    )
    version ='unknown'
    with open('../version.py', 'r') as v:
        version = v.read().split('=')[1]
    version = f'{version}.base'
    config_template = _create_base_config_template()
    base_fs = get_fs()
    for minicube_location in minicube_locations.iterrows():
        tc = copy.deepcopy(config_template)
        base_config = _create_base_config(minicube_location, tc, version)
        data_id = base_config.get('properties').get('data_id')
        with base_fs.open(
                f'deepextremes-minicubes/configs/base/{version}/'
                f'{data_id}.geojson', 'wb') as mc_json:
            mc_json.write(json.dumps(tc, indent=2).encode('utf-8'))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError('Not exactly one argument given: this function requires a '
                         'path to a file containing event locations as created in'
                         'step 01-location-extraction.')
    location_file = sys.argv[1]
    _generate_base_configs(location_file)