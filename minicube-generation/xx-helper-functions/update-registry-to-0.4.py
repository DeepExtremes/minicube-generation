import fsspec
import geopandas as gpd
import json
import os
import pandas as pd

from typing import List

MC_REGISTRY_V3 = 'deepextremes-minicubes/mc_registry_v3.csv'
MC_REGISTRY_V4 = 'deepextremes-minicubes/mc_registry_v4.csv'


location_files = [
    ('csv', 'csv_events', 'MinicubeLocation_event_v1.csv'),
    ('csv', 'csv_non_events', 'MinicubeLocation_nonEvent_v1.csv'),
    ('json', 'json_pure_non_events', 'sampling_purelc_nonevent_location.json'),
    ('json', 'json_pure_events', 'sampling_purelc_event_location.json'),
    # ('json', 'json_mixed_non_events', 'sampling_mixedlc_nonevent_location.json'),
    # ('json', 'json_mixed_events', 'sampling_mixedlc_event_location.json'),
]
source_columns = ['mc_id', 'version', 'location_id', 'class']


def _replace_class_dict(dfs: List[dict]) -> List[dict]:
    for df in dfs:
        if df['Class'] == 1.0:
            df['Class'] = 'broadtree'
        elif df['Class'] == 2.0:
            df['Class'] = 'grassland'
        elif df['Class'] == 3.0:
            df['Class'] = 'mixedtree'
        elif df['Class'] == 4.0:
            df['Class'] = 'needletree'
        elif df['Class'] == 5.0:
            df['Class'] = 'soil'
        elif df['Class'] == 6.0:
            df['Class'] = 'urban'
    return dfs


def _replace_class(df: pd.DataFrame) -> pd.DataFrame:
    df_subset = df.Class.replace(1.0, "broadtree")
    df_subset = df_subset.replace(2.0, "grassland")
    df_subset = df_subset.replace(3.0, "mixedtree")
    df_subset = df_subset.replace(4.0, "needletree")
    df_subset = df_subset.replace(5.0, "soil")
    df_subset = df_subset.replace(6.0, "urban")
    df.Class = df_subset
    return df


def _prepare_csv(df: pd.DataFrame) -> pd.DataFrame:
    location_ids = df.Longitude.map('{:,.2f}'.format) + \
                   '_' + df.Latitude.map('{:,.2f}'.format)
    df.insert(2, 'location_ids', location_ids)
    if 'EventStart' not in df:
        df['EventStart'] = 'not'
    if 'EventEnd' not in df:
        df['EventEnd'] = 'not'
    if 'EventLabel' not in df:
        df['EventLabel'] = 'not'
    return df


def _add_location_ids_dict(dfs: List[dict]) -> List[dict]:
    for df in dfs:
        df['LocationId'] = '{:,.2f}'.format(df['Longitude']) + '_' + \
                           '{:,.2f}'.format(df['Latitude'])
    return dfs


def _get_df_from_csv(location_file: str, mc_reg: gpd.GeoDataFrame, fs) \
        -> pd.DataFrame:
    with fs.open(f'deepextremes-minicubes/input_events/{location_file}',
                 'r') as gjlocs:
        event_locations = gpd.GeoDataFrame(pd.read_csv(gjlocs))
    event_locations = _replace_class(event_locations)
    event_locations = _prepare_csv(event_locations)
    # remove locations which are already included in the minicube registry
    included_cubes = \
        mc_reg.loc[mc_reg['location_id'].isin(
            event_locations['location_ids'].array)][source_columns]
    included_cubes['source'] = location_file
    return included_cubes


def _get_df_from_json(location_file: str, mc_reg: gpd.GeoDataFrame, fs) \
        -> pd.DataFrame:
    with fs.open(f'deepextremes-minicubes/input_events/{location_file}',
                 'r') as gjlocs:
        event_locations = json.load(gjlocs)
        event_locations = [value for key, value in event_locations.items()]
    event_locations = _replace_class_dict(event_locations)
    event_locations = _add_location_ids_dict(event_locations)
    included_cubes = pd.DataFrame(columns=source_columns)
    for event_location in event_locations:
        included_cube = \
            mc_reg.loc[mc_reg['location_id'] ==
                       event_location['LocationId']][source_columns]
        if len(included_cube) > 0:
            included_cubes = pd.DataFrame.append(included_cubes, included_cube)
    included_cubes['source'] = location_file
    return included_cubes


def _check_location_sources():
    s3_key = os.environ["S3_USER_STORAGE_KEY"]
    s3_secret = os.environ["S3_USER_STORAGE_SECRET"]
    storage_options = dict(
        anon=False,
        key=s3_key,
        secret=s3_secret
    )
    fs = fsspec.filesystem('s3', **storage_options)
    with fs.open(MC_REGISTRY_V3, 'r') as gjreg:
        mc_reg = gpd.GeoDataFrame(pd.read_csv(gjreg))
    print(len(mc_reg))
    cubes = pd.DataFrame(columns=[source_columns])
    for location_file in location_files:
        print(f'Examining {location_file}')
        if location_file[0] == 'json':
            new_cubes = _get_df_from_json(location_file[2], mc_reg, fs)
            for new_cube in new_cubes.iterrows():
                location_id = new_cube[1]['location_id']
                previous_cubes_at_this_location = \
                    cubes.loc[cubes['location_id'] == location_id]
                if len(previous_cubes_at_this_location) > 0:
                    previous_source = previous_cubes_at_this_location.iloc[0, 5]
                    new_cubes.at[new_cube[0], 'source'] = f"{new_cube[1]['source']}, {previous_source}"
                    cubes = cubes.drop(new_cube[0])
            cubes = pd.DataFrame.append(cubes, new_cubes)
        elif location_file[0] == 'csv':
            new_cubes = _get_df_from_csv(location_file[2], mc_reg, fs)

            cubes = pd.DataFrame.append(cubes, new_cubes)
    cubes.to_csv('minicube_sources.csv', index=False, sep='\t')


if __name__ == "__main__":
    _check_location_sources()
