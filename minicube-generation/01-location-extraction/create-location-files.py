import fsspec
import geopandas as gpd
import json
import os
import pandas as pd

from typing import List

from constants import MC_REGISTRY

location_files = [
    ('json', 'json_pure_non_events', 'sampling_purelc_nonevent_location.json'),
    ('json', 'json_pure_events', 'sampling_purelc_event_location.json'),
    ('json', 'json_mixed_non_events', 'sampling_mixedlc_nonevent_location.json'),
    ('json', 'json_mixed_events', 'sampling_mixedlc_event_location.json'),
]


def _replace_class_dict(dfs: List[dict]) -> List[dict]:
    for df in dfs:
        if 'Dominant_Class' in df:
            df['Class'] = '0'
            df['DominantClass'] = df.pop('Dominant_Class')
            df['SecondDominantClass'] = df.pop('2nd_Dominant_Class')
        elif '2nd_Class' in df:
            df['DominantClass'] = df.pop('Class')
            df['SecondDominantClass'] = df.pop('2nd_Class')
            df['Class'] = '0'
        else:
            df['DominantClass'] = '0'
            df['SecondDominantClass'] = '0'
        for klass in ['Class', 'DominantClass', 'SecondDominantClass']:
            if klass in df:
                if int(df[klass]) == 0:
                    df[klass] = 'not'
                elif int(df[klass]) == 1:
                    df[klass] = 'broadtree'
                elif int(df[klass]) == 2:
                    df[klass] = 'grassland'
                elif int(df[klass]) == 3:
                    df[klass] = 'mixedtree'
                elif int(df[klass]) == 4:
                    df[klass] = 'needletree'
                elif int(df[klass]) == 5:
                    df[klass] = 'soil'
                elif int(df[klass]) == 6:
                    df[klass] = 'urban'
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


def _drop_days(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop('EventDays', axis=1)


def _add_location_ids_dict(dfs: List[dict]) -> List[dict]:
    for df in dfs:
        df['LocationId'] = '{:,.2f}'.format(df['Longitude']) + '_' + \
                           '{:,.2f}'.format(df['Latitude'])
    return dfs


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


def _prepare_dict(dfs: List[dict]) -> List[dict]:
    for df in dfs:
        if 'EventDays' in df:
            df.pop('EventDays')
        if 'Event_Days' in df:
            df.pop('Event_Days')
        df.pop('LocationId')
        if 'EventStart' not in df:
            df['EventStart'] = []
        if 'EventEnd' not in df:
            df['EventEnd'] = []
        if 'EventLabel' not in df:
            df['EventLabel'] = []
    return dfs


def _harmonize_events(dfs: List[dict]) -> List[dict]:
    for df in dfs:
        label = df['EventLabel']
        if label != []:
            df['EventLabel'] = [int(l) for l in label]
        start = df['EventStart']
        formatted_start_times = []
        for start_time in start:
            formatted_start_times.append(
                pd.Timestamp(start_time).strftime('%Y-%m-%d')
            )
        df['EventStart'] = formatted_start_times
        end = df['EventEnd']
        formatted_end_times = []
        for end_time in end:
            formatted_end_times.append(
                pd.Timestamp(end_time).strftime('%Y-%m-%d')
            )
        df['EventEnd'] = formatted_end_times
    return dfs

def _get_output_df_from_csv(location_file: str, mc_reg: gpd.GeoDataFrame, fs) \
        -> pd.DataFrame:
    with fs.open(f'deepextremes-minicubes/input_events/{location_file}',
                 'r') as gjlocs:
        event_locations = gpd.GeoDataFrame(pd.read_csv(gjlocs))
    event_locations = _drop_days(event_locations)
    event_locations = _replace_class(event_locations)
    event_locations = _prepare_csv(event_locations)
    # remove locations which are already included in the minicube registry
    remaining_event_locations = event_locations.loc[
        ~event_locations['location_ids'].isin(mc_reg['location_id'].array)]
    remaining_event_locations = remaining_event_locations.drop('location_ids',
                                                               axis=1)
    return remaining_event_locations.reindex(
        columns=['Longitude', 'Latitude', 'Class', 'EventStart', 'EventEnd',
                 'EventLabel', 'OutsideEvent']
    )


def _get_output_df_from_json(location_file: str, mc_reg: gpd.GeoDataFrame, fs) \
        -> pd.DataFrame:
    with fs.open(f'deepextremes-minicubes/input_events/{location_file}',
                 'r') as gjlocs:
        event_locations = json.load(gjlocs)
        event_locations = [value for key, value in event_locations.items()]
    event_locations = _replace_class_dict(event_locations)
    event_locations = _add_location_ids_dict(event_locations)
    remaining_event_locations = []
    for event_location in event_locations:
        if event_location['LocationId'] not in mc_reg['location_id'].array:
            remaining_event_locations.append(event_location)
        event_location['LocationSource'] = location_file
    remaining_event_locations = _prepare_dict(remaining_event_locations)
    remaining_event_locations = _harmonize_events(remaining_event_locations)
    output_df = pd.DataFrame(columns=['Longitude', 'Latitude', 'Class',
                                      'DominantClass', 'SecondDominantClass',
                                      'EventStart', 'EventEnd',
                                      'EventLabel', 'OutsideEvent',
                                      'LocationSource'])
    for remaining_event_location in remaining_event_locations:
        output_df = output_df.append(remaining_event_location,
                                     ignore_index=True)
    return output_df


def _create_location_files():
    s3_key = os.environ["S3_USER_STORAGE_KEY"]
    s3_secret = os.environ["S3_USER_STORAGE_SECRET"]
    storage_options = dict(
        anon=False,
        key=s3_key,
        secret=s3_secret
    )
    fs = fsspec.filesystem('s3', **storage_options)
    with fs.open(MC_REGISTRY, 'r') as gjreg:
        mc_reg = gpd.GeoDataFrame(pd.read_csv(gjreg))
    for location_file in location_files:
        if location_file[0] == 'json':
            output_df = _get_output_df_from_json(location_file[2], mc_reg, fs)
        elif location_file[0] == 'csv':
            output_df = _get_output_df_from_csv(location_file[2], mc_reg, fs)
        output_df.to_csv(f'{location_file[1]}.csv', index=False, sep='\t')


if __name__ == "__main__":
    _create_location_files()
