import fsspec
import geopandas as gpd
import os
import pandas as pd

_MC_REGISTRY = 'mc_registry_v3.csv'
location_files = [('inside_events', 'MinicubeLocation_test2.csv'),
                  ('outside_events', 'MinicubeLocation_nonEvent_v1.csv')]


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


def _add_location_ids(df: pd.DataFrame) -> pd.DataFrame:
    location_ids = df.Longitude.map('{:,.2f}'.format) + \
                   '_' + df.Latitude.map('{:,.2f}'.format)
    df.insert(2, 'location_ids', location_ids)
    return df


def _create_location_files():
    s3_key = os.environ["S3_USER_STORAGE_KEY"]
    s3_secret = os.environ["S3_USER_STORAGE_SECRET"]
    storage_options = dict(
        anon=False,
        key=s3_key,
        secret=s3_secret
    )
    fs = fsspec.filesystem('s3', **storage_options)
    with fs.open(f'deepextremes-minicubes/{_MC_REGISTRY}', 'r') as gjreg:
        mc_reg = gpd.GeoDataFrame(pd.read_csv(gjreg))
    for location_file in location_files:
        with fs.open(f'deepextremes-minicubes/input_events/{location_file[1]}', 'r') as gjlocs:
            event_locations = gpd.GeoDataFrame(pd.read_csv(gjlocs))
        event_locations = _drop_days(event_locations)
        event_locations = _replace_class(event_locations)
        event_locations = _add_location_ids(event_locations)
        # remove locations which are already included in the minicube registry
        remaining_event_locations = event_locations.loc[~event_locations['location_ids'].isin(mc_reg['location_id'].array)]
        remaining_event_locations = remaining_event_locations.drop('location_ids', axis=1)
        remaining_event_locations = remaining_event_locations.reindex(
            columns=['Longitude', 'Latitude', 'Class', 'EventStart', 'EventEnd',
                     'EventLabel', 'OutsideEvent']
        )
        remaining_event_locations.to_csv(f'{location_file[0]}.csv', index=False, sep='\t')


if __name__ == "__main__":
    _create_location_files()
