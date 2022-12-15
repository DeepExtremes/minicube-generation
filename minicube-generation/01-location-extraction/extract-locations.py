from datetime import datetime
import os
import sys
import xarray as xr

LC_CLASSES = {
    1: 'Grassland',
    2: 'Mixed Tree',
    3: 'Needleleaved Tree',
    4: 'Broadleaved Tree',
    5: 'Bare Areas',
    6: 'Urban Area'
}


def extract_locations(min_lon: float, max_lon:float,
                      min_lat: float, max_lat: float):
    location_files = os.listdir("../Locations/")
    location_files.sort()
    all_locations = []
    for i, location_file in enumerate(location_files):
        ds = xr.open_zarr(location_file)
        layer = ds.layer.sel(lon=slice(min_lon, max_lon),
                             lat=slice(min_lat, max_lat))
        layer_locations = _extract_locations_from_layer(layer, i + 1)
        all_locations.extend(layer_locations)

    version ='unknown'
    with open('locationversions.py', 'r') as v:
        version = v.read()
    date = datetime.now().strftime('%Y-%m-%d')
    filename = f'minicube_locations_v{version}_{date}_' \
               f'({min_lon}_{max_lon}_{min_lat}_{max_lat}).csv'
    with open(filename, 'w+') as output:
        output.write("Longitude\tLatitude\tClass")
        for location in all_locations:
            output.write(f'{location[0]}\t'
                         f'{location[1]}\t'
                         f'{location[2]}')


def _extract_locations_from_layer(da: xr.Dataarray, cci_layer_id: int):
    layer_frame = da.squeeze().to_dataframe(name='layer')
    layer_locations = []
    for layer in range(1, 7):
        sample_frame = layer_frame[layer_frame['layer'] == layer].sample(n=1)
        lat, lon = sample_frame.index[0]
        layer_locations.append(
            (lon, lat, f'{LC_CLASSES.get(cci_layer_id)}_{layer}')
        )
    return layer_locations


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise ValueError('Expected coordinates '
                         'MIN_LON, MAX_LON, MIN_LAT, MAX_LAT')
    else:
        coords = sys.argv[1:]
        extract_locations(float(coords[0]), float(coords[1]),
                          float(coords[2]), float(coords[3]))
