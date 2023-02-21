from datetime import datetime
import fsspec
import numpy as np
import pandas as pd
import sys
import xarray as xr


_TIME_FORMAT = '%Y-%m-%d'


def _get_random_pos(ds: xr.DataArray, coord: str):
    extreme_0 = ds[coord][0].values
    extreme_1 = ds[coord][-1].values
    if extreme_0 < extreme_1:
        return extreme_0 + ((extreme_1 - extreme_0) * np.random.random())
    return extreme_1 + ((extreme_0 - extreme_1) * np.random.random())


def _get_random_point(ds: xr.DataArray):
    lat = _get_random_pos(ds, 'latitude')
    lon = _get_random_pos(ds, 'longitude')
    return lon, lat


def extract_event_locations(min_lon: float, max_lon: float,
                            min_lat: float, max_lat: float):
    storage_options = dict(
        anon=True,
        client_kwargs=dict(
            endpoint_url='https://s3.bgc-jena.mpg.de:9000/',
        )
    )
    fs = fsspec.filesystem('s3', **storage_options)
    event_stats = fs.open(
        'xaida/v2/EventStats_ranked_pot0.01_ne0.1_tcmp_Sdiam3_T5_2016_2021.csv'
    )
    events_frame = pd.read_csv(event_stats, delimiter=',')

    # collecting data from events that are entirely within the specified bounds
    sub_frame = events_frame[events_frame.longitude_min > min_lon]
    sub_frame = sub_frame[sub_frame.longitude_max < max_lon]
    sub_frame = sub_frame[sub_frame.latitude_min > min_lat]
    sub_frame = sub_frame[sub_frame.latitude_max < max_lat]

    # consider events with a certain minimal size for the moment
    sub_frame = sub_frame[sub_frame.area > 7.]

    label_cube_store = fsspec.get_mapper(
        'https://s3.bgc-jena.mpg.de:9000/xaida/v2/'
        'labelcube_ranked_pot0.01_ne0.1_tcmp_Sdiam3_T5_2016_2021.zarr'
    )
    label_cube = xr.open_zarr(label_cube_store)

    all_locations = []

    num_valid_events = 0

    for index, line in sub_frame.iterrows():
        start_time = line['start_time']
        end_time = line['end_time']
        longitude_min = line['longitude_min']
        longitude_max = line['longitude_max']
        latitude_min = line['latitude_min']
        latitude_max = line['latitude_max']
        label = line['label']

        print(f'Investigate Event {label}')

        if latitude_max == latitude_min or longitude_max == longitude_min:
            # we need area
            continue

        sub_label = label_cube.layer.sel(
            {
                'time': slice(start_time,
                              end_time),
                'longitude': slice(float(longitude_min) - 1.0,
                                   float(longitude_max) + 1.0),
                'latitude': slice(float(latitude_max) + 1.0,
                                  float(latitude_min) - 1.0)
            }
        )
        num_event_locations_found = 0
        while num_event_locations_found < 20:
            random_point = _get_random_point(sub_label)
            event_occurrences = np.where(sub_label.sel(
                {
                    'longitude': random_point[0],
                    'latitude': random_point[1]
                },
                method='nearest').values == int(label))[0]
            if len(event_occurrences) == 0:
                continue
            print(f'Found valid location at long {random_point[0]}, '
                  f'lat {random_point[1]}')
            event_start = sub_label.time[event_occurrences[0]].values
            event_start = pd.to_datetime(event_start).strftime(_TIME_FORMAT)
            event_end = sub_label.time[event_occurrences[-1]].values
            event_end = pd.to_datetime(event_end).strftime(_TIME_FORMAT)
            location = (random_point[0] - 180., random_point[1], '_',
                        event_start, event_end, label)
            all_locations.append(location)
            num_event_locations_found += 1
        num_valid_events += 1
        if num_valid_events > 2:
            break

    version ='unknown'
    with open('locationversions.py', 'r') as v:
        version = v.read().split('=')[-1]
    date = datetime.now().strftime('%Y-%m-%d')
    filename = f'../minicube_locations_v{version}_{date}_' \
               f'{min_lon - 180}_{max_lon - 180}_{min_lat}_{max_lat}.csv'
    with open(filename, 'w+') as output:
        output.write(
            "Longitude\tLatitude\tClass\tEventStart\tEventEnd\tEventLabel\n"
        )
        for location in all_locations:
            output.write(f'{location[0]}\t'
                         f'{location[1]}\t'
                         f'{location[2]}\t'
                         f'{location[3]}\t'
                         f'{location[4]}\t'
                         f'{location[5]}\n')


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise ValueError('Expected coordinates '
                         'MIN_LON, MAX_LON, MIN_LAT, MAX_LAT')
    else:
        coords = sys.argv[1:]
        extract_event_locations(float(coords[0]), float(coords[1]),
                                float(coords[2]), float(coords[3]))
