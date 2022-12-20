from datetime import datetime
import glob
import json
import math
import numpy as np
import pandas as pd
import sys

from pyproj import CRS
from pyproj import Proj

_ID_TEMPLATE = "mc_{lon}_{lat}_{version}"
_TITLE_TEMPLATE = "Minicube at {lon} {lat}"
_DESCRIPTION_TEMPLATE = "The minicube covering the area around {lon} and {lat}"
_SPATIAL_RES = 20
_HALF_RES = 64 * _SPATIAL_RES


def _get_crs(lon: float, lat: float) -> str:
    utm_zone = int(math.floor(lon + 180) / 6)
    hemisphere = "south" if lat < 0 else "north"
    crs = CRS.from_string(f'+proj=utm +zone={utm_zone} +{hemisphere}')
    epsg_code = crs.to_epsg()
    return f'EPSG:{epsg_code}'


def generate_minicube_configs(location_file: str):
    minicube_locations = pd.read_csv(location_file, delimiter="\t")
    version ='unknown'
    with open('../version.py', 'r') as v:
        version = v.read()
    with open('../cube.geojson', 'r') as template:
        t = json.load(template)
        for minicube_location in minicube_locations.iterrows():
            center_lon = minicube_location[1].Longitude
            center_lon_readable = "{:.2f}".format(center_lon)
            center_lat = minicube_location[1].Latitude
            center_lat_readable = "{:.2f}".format(center_lat)
            crs = _get_crs(center_lon, center_lat)
            utm_proj = Proj(crs)
            x, y = utm_proj(center_lon, center_lat)
            xmin = x - _HALF_RES
            xmax = x + _HALF_RES
            ymin = y - _HALF_RES
            ymax = y + _HALF_RES
            lons, lats = utm_proj(
                (xmin, xmin, xmax, xmax, xmin),
                (ymin, ymax, ymax, ymin, ymin),
                inverse=True
            )
            geospatial_bbox = np.swapaxes(np.asarray((lons, lats)), 0, 1).tolist()
            t['geometry']['coordinates'][0] = geospatial_bbox
            t['properties']['title'] = _TITLE_TEMPLATE.\
                format(lon=center_lon_readable, lat=center_lat_readable)
            data_id = _ID_TEMPLATE.format(lon=center_lon_readable,
                                          lat=center_lat_readable,
                                          version=version)
            t['properties']['data_id'] = data_id
            t['properties']['description'] = _DESCRIPTION_TEMPLATE.\
                format(lon=center_lon_readable, lat=center_lat_readable)
            t['properties']['version'] = version
            t['properties']['spatial_res'] = _SPATIAL_RES
            t['properties']['spatial_ref'] = crs
            t['properties']['spatial_bbox'] = [xmin, ymin, xmax, ymax]
            t['properties']['metadata']['date_modified'] = \
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            t['properties']['metadata']['geospatial_lon_min'] = min(lons)
            t['properties']['metadata']['geospatial_lon_max'] = max(lons)
            t['properties']['metadata']['geospatial_lat_min'] = min(lats)
            t['properties']['metadata']['geospatial_lat_max'] = max(lats)
            with open(f'../configs/{data_id}.geojson', 'w+') as mc_json:
                json.dump(t, mc_json, indent=4)


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
