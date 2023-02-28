from datetime import datetime
import geopandas as gpd
import json
import fsspec
import os
import rioxarray
from shapely.geometry import Polygon
import sys
import xarray as xr
from typing import Dict

from xcube.core.gridmapping import GridMapping
from xcube.core.store import new_data_store
from xcube.core.mldataset import MultiLevelDataset
from xcube.core.resampling import resample_in_space
from xcube.core.update import update_dataset_chunk_encoding

from maskaycloudmask import compute_cloud_mask

_MONTHS = dict(
    jan=1, feb=2, mar=3, apr=4, may=5, jun=6,
    jul=7, aug=8, sep=9, oct=10, nov=11, dec=12
)


def _get_source_datasets(source_config: dict,
                         aws_access_key_id: str,
                         aws_secret_access_key: str,
                         mc_config: dict) -> Dict[str, xr.Dataset]:
    datasets = {}
    if 'store_id' in source_config:
        if source_config.get('store_params', {}).get('storage_options', {}).\
                get('anon') == "True":
            source_config['store_params']['storage_options']['anon'] = True
        elif source_config.get('store_params', {}).get('storage_options', {}).\
                get('anon') == "False":
            source_config['store_params']['storage_options']['anon'] = False
        if source_config.get('store_params', {}).get('storage_options', {}).\
                get('client_kwargs', {}).get('aws_access_key_id') == "":
            source_config['store_params']['storage_options']['client_kwargs']['aws_access_key_id'] = aws_access_key_id
        if source_config.get('store_params', {}).get('storage_options', {}).\
                get('client_kwargs', {}).get('aws_secret_access_key') == "":
            source_config['store_params']['storage_options']['client_kwargs']['aws_secret_access_key'] = aws_secret_access_key
        source_store = new_data_store(
            source_config['store_id'],
            **source_config.get('store_params', {})
        )
        for dataset_name, variable_names in source_config['datasets'].items():
            open_schema = \
                source_store.get_open_data_params_schema(dataset_name)
            open_properties = \
                list(open_schema.to_dict().get('properties').keys())
            props = source_config.get('open_params', {})
            if 'variable_names' in open_properties:
                props['variable_names'] = variable_names['variable_names']
            if 'bbox' in open_properties:
                props['bbox'] = mc_config['properties']['spatial_bbox']
            if 'time_range' in open_properties:
                props['time_range'] = mc_config['properties']['time_range']
            if 'time_period' in open_properties:
                props['time_period'] = mc_config['properties']['time_period']
            if 'crs' in open_properties:
                props['crs'] = mc_config['properties']['spatial_ref']
            if 'spatial_res' in open_properties:
                props['spatial_res'] = mc_config['properties']['spatial_res']
            source_ds = source_store.open_data(
                dataset_name,
                **props
            )
            if isinstance(source_ds, MultiLevelDataset):
                source_ds = source_ds.base_dataset
            if 'variable_names' not in open_properties:
                non_requested_vars = []
                for var in source_ds.data_vars:
                    if var not in variable_names['variable_names']:
                        non_requested_vars.append(var)
                source_ds = source_ds.drop_vars(non_requested_vars)
            datasets[dataset_name] = source_ds
    if 'filesystem' in source_config:
        if source_config.get('storage_options', {}).\
                get('anon') == "True":
            source_config['storage_options']['anon'] = True
        elif source_config.get('storage_options', {}).\
                get('anon') == "False":
            source_config['storage_options']['anon'] = False
        fs = fsspec.filesystem(
            source_config['filesystem'],
            **source_config.get('storage_options')
        )
        for dataset_name in source_config['datasets'].keys():
            if 'path' in source_config['datasets'][dataset_name]:
                mapper = fsspec.get_mapper(
                    source_config['datasets'][dataset_name]['path']
                )
                datasets[dataset_name] = xr.open_zarr(mapper)
            else:
                data_arrays = []
                for var_name, da_dict in \
                        source_config['datasets'][dataset_name]['dataarrays'].\
                                items():
                    file_like = fs.open(da_dict['path'])
                    data_arrays.append(
                        rioxarray.open_rasterio(file_like).rename(var_name)
                    )
                datasets[dataset_name] = xr.merge(data_arrays)
    return datasets


def _execute_processing_step(processing_step: str,
                             ps_ds: xr.Dataset,
                             additional_sources: dict,
                             mc_config: dict
                            ) -> xr.Dataset:
    # Add processing step implementations as necessary
    params_string = processing_step.split('/ ')[1] \
        if '/' in processing_step \
        else None
    if processing_step.startswith('Resample spatially to '):
        return _resample_spatially(
            ds_source=ps_ds,
            ds_target=additional_sources['resampling_target'],
            target_crs=mc_config['properties']['spatial_ref'],
            spatial_resolution=int(params_string) if params_string else None
        )
    if processing_step.startswith('Resample temporally to '):
        return _resample_temporally(
            ds_source=ps_ds,
            ds_target=additional_sources['resampling_target'],
            time_range=mc_config['properties']['time_range'],
            time_period=mc_config['properties']['time_period'],
            resampling_method=params_string
        )
    if processing_step.startswith('Move longitude'):
        return _move_longitude(
            ds_source=ps_ds
        )
    if processing_step.startswith('Subset temporally'):
        return _subset_temporally(
            ds_source=ps_ds,
            time_range=mc_config['properties']['time_range']
        )
    if processing_step.startswith('Pick time value'):
        return _pick_time_value(ps_ds, index=int(params_string))
    if processing_step.startswith('Subset spatially around center'):
        _, center_lon, center_lat, _ = \
            mc_config['properties']['data_id'].split('_')
        return _subset_spatially_around_center(
            ds=ps_ds,
            center_lat=float(center_lat),
            center_lon=float(center_lon),
            num_degrees=int(params_string) if params_string else None
        )
    if processing_step.startswith('Pick /'):
        return _pick(ps_ds, ending=params_string)
    if processing_step == 'Pick center spatial value':
        _, center_lon, center_lat, _ = \
            mc_config['properties']['data_id'].split('_')
        return _pick_center_spatial_value(
            ds_source=ps_ds,
            center_lat=float(center_lat),
            center_lon=float(center_lon)
        )
    if processing_step.startswith('Rename from to /'):
        before, after = params_string.split(' ')
        return _rename_from_to(
            ds_source=ps_ds,
            before=before,
            after=after
        )
    if processing_step.startswith('Chunk by time /'):
        return _chunk_by_time(
            ds_source=ps_ds,
            time_chunk_size=int(params_string)
        )
    if processing_step.startswith('Adjust bound coordinates'):
        return _adjust_bound_coordinates(
            ds_source=ps_ds
        )
    if processing_step.startswith('Aggregate monthly /'):
        return _aggregate_monthly(
            ds_source=ps_ds,
            aggregated_var_name=params_string
        )
    if processing_step.startswith('Compute Cloud Mask (maskay)'):
        return compute_cloud_mask(ds_source=ps_ds)
    raise ValueError(f'Processing step "{processing_step}" not found')


def _finalize_dataset(ds: xr.Dataset, mc_config: dict) -> xr.Dataset:
    variable_properties = mc_config['properties'].pop('variables')
    mc_config['properties'].pop('sources')
    vc_dict = {vc['name']: vc for vc in variable_properties}
    ds = ds.assign_attrs(**mc_config['properties'])
    ds = ds.assign_attrs({
        'creation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })
    for variable_name, attrs in vc_dict.items():
        ds[variable_name] = ds[variable_name].assign_attrs(attrs)
    return ds


def _write_ds(ds: xr.Dataset, mc_config: dict):
    print(f'Writing dataset {ds.data_id}')
    s3_key = os.environ["S3_USER_STORAGE_KEY"]
    s3_secret = os.environ["S3_USER_STORAGE_SECRET"]
    s3_store = new_data_store(
        "s3",
        root="deepextremes-minicubes",
        storage_options=dict(
            anon=False,
            key=s3_key,
            secret=s3_secret
        )
    )
    version = mc_config['properties']['version']
    s3_store.write_data(ds, f"{version}/{ds.data_id}.zarr")
    # enable to write local
    # ds.to_zarr(f"{version}/{ds.data_id}.zarr")
    print(f'Finished writing dataset {ds.data_id}')


def _get_gdf_from_mc(mc: xr.Dataset) -> gpd.GeoDataFrame:
    mc_id = mc.attrs.get('data_id')
    version = mc.attrs.get('version')
    path = f'deepextremes-minicubes/{version}/{mc_id}.zarr'
    creation_date = mc.attrs.get('creation_date')
    events = str([(
        mc.attrs.get('metadata', {}).get('event_label'),
        mc.attrs.get('metadata', {}).get('event_start_time'),
        mc.attrs.get('metadata', {}).get('event_end_time')
    )])

    lon_min = mc.attrs.get('metadata', {}).get('geospatial_lon_min')
    lon_max = mc.attrs.get('metadata', {}).get('geospatial_lon_max')
    lat_min = mc.attrs.get('metadata', {}).get('geospatial_lat_min')
    lat_max = mc.attrs.get('metadata', {}).get('geospatial_lat_max')

    geometry = Polygon([[lon_min, lat_min], [lon_max, lat_min], [lon_max, lat_max], [lon_min, lat_max]])
    reg_gdf = gpd.GeoDataFrame(
        {
            'mc_id': mc_id,
            'path': path,
            'version': version,
            'geometry': geometry,
            'creation_date': creation_date,
            'events': events,
            'remarks': None
        },
        index=[0]
    )
    return reg_gdf


def _write_entry(ds: xr.Dataset):
    s3_key = os.environ["S3_USER_STORAGE_KEY"]
    s3_secret = os.environ["S3_USER_STORAGE_SECRET"]
    storage_options = dict(
        anon=False,
        key=s3_key,
        secret=s3_secret
    )
    fs = fsspec.filesystem('s3', **storage_options)
    gdf = _get_gdf_from_mc(ds)
    with fs.open('deepextremes-minicubes/mc_registry.csv', 'a') as registry:
        registry.write(gdf.to_csv(header=False, index=False))


def generate_cube(mc_config: dict,
                  aws_access_key_id: str,
                  aws_secret_access_key:str
                  ):
    print(f'Processing minicube configuration '
          f'{mc_config["properties"]["data_id"]}')
    variable_configs = mc_config['properties']['variables']
    processing_steps_set = set()
    for vc in variable_configs:
        ps = ";".join(vc.get('sources', [])[0].get('processing_steps', []))
        processing_steps_set.add(ps)

    sources = mc_config['properties']['sources']
    datasets = {}
    for source in sources:
        print(f'Read {source["name"]}')
        datasets.update(
            _get_source_datasets(
                source, aws_access_key_id,
                aws_secret_access_key, mc_config
            )
        )

    output_datasets = []
    for i, processing_steps_s in enumerate(processing_steps_set):
        processing_steps = processing_steps_s.split(';')
        ps1_ds_name = processing_steps[0].split('Read ')[1]
        if ps1_ds_name not in datasets:
            raise ValueError(f'Invalid processing step: '
                             f'Dataset {ps1_ds_name} expected, '
                             f'but was not found in sources')
        print(f'Processing {i + 1} of {len(processing_steps_set)}')
        ps_ds = datasets[ps1_ds_name]
        for processing_step in processing_steps[1:]:
            print(processing_step)
            additional_sources = {}
            if processing_step.startswith('Resample'):
                resampling_target = datasets[
                    processing_step.split('to ')[1].split(' /')[0]]
                additional_sources['resampling_target'] = resampling_target
            ps_ds = _execute_processing_step(processing_step, ps_ds,
                                             additional_sources, mc_config)
        output_datasets.append(ps_ds)

    ds = xr.merge(output_datasets)

    ds = _finalize_dataset(ds, mc_config)

    _write_ds(ds, mc_config)

    _write_entry(ds)

# processing step implementations


def _adjust_bound_coordinates(ds_source: xr.Dataset) -> xr.Dataset:
    bounds_vars = [data_var for data_var in ds_source.data_vars
                   if 'bounds' in data_var or 'bnds' in data_var]
    return ds_source.set_coords(bounds_vars)


def _aggregate_monthly(ds_source: xr.Dataset, aggregated_var_name: str) \
        -> xr.Dataset:
    data_arrays = []
    for data_var in ds_source.data_vars:
        month = data_var.split('_')[-1]
        data_arrays.append(xr.DataArray(
            name=aggregated_var_name,
            data=ds_source[data_var],
            coords={'month': [_MONTHS[month]],
                    "y": ds_source.y,
                    "x": ds_source.x},
            dims=["month", "y", "x"]
        ))
    ds = xr.combine_by_coords(data_arrays)
    return ds.assign_coords(spatial_ref=ds_source.spatial_ref)


def _chunk_by_time(ds_source: xr.Dataset, time_chunk_size:int) -> xr.Dataset:
    ds = ds_source
    for data_var in ds.data_vars:
        if 'time' in ds[data_var].dims:
            ds[data_var] = ds[data_var].chunk(chunks={'time': time_chunk_size})
            ds = update_dataset_chunk_encoding(ds,
                                               chunk_sizes={'time': time_chunk_size},
                                               format_name='zarr')
    return ds


def _move_longitude(ds_source: xr.Dataset) -> xr.Dataset:
    return ds_source.assign(longitude=ds_source.longitude - 180.0)


def _pick(ds: xr.Dataset, ending: str) -> xr.Dataset:
    vars_to_be_removed = []
    for var in ds.data_vars:
        if not var.endswith(ending):
            vars_to_be_removed.append(var)
    return ds.drop_vars(vars_to_be_removed)


def _pick_center_spatial_value(ds_source: xr.Dataset,
                               center_lat: float,
                               center_lon: float) -> xr.Dataset:
    if 'lat' in ds_source.coords and 'lon' in ds_source.coords:
        ds = ds_source.sel(lat=center_lat, lon=center_lon, method='nearest')
        return ds.drop_vars(['lat', 'lon'])
    ds = ds_source.sel(latitude=center_lat,
                       longitude=center_lon,
                       method='nearest')
    return ds.drop_vars(['latitude', 'longitude'])


def _pick_time_value(ds: xr.Dataset, index: int) -> xr.Dataset:
    return ds.isel({'time': index})


def _rename_from_to(ds_source: xr.Dataset, before:str, after: str) \
        -> xr.Dataset:
    return ds_source.rename({before: after})


def _resample_spatially(ds_source: xr.Dataset, ds_target: xr.Dataset,
                        target_crs: str, spatial_resolution: int = None) \
        -> xr.Dataset:
    target_gm = GridMapping.from_dataset(ds_target)
    adjusted_target_gm = None
    if spatial_resolution:
        target_res = target_gm.x_res
        scale = target_res / spatial_resolution
        adjusted_target_gm = target_gm.scale(scale)
    try:
        ds = ds_source.rio.reproject(target_crs)
    except rioxarray.exceptions.MissingCRS:
        source_gm = GridMapping.from_dataset(ds_source)
        ds = ds_source.rio.write_crs(source_gm.crs)
        ds = ds.rio.reproject(target_gm.crs)
    ds = resample_in_space(ds, target_gm=target_gm)
    if adjusted_target_gm:
        ds = resample_in_space(ds, target_gm=adjusted_target_gm)
        new_gm = GridMapping.from_dataset(ds)
        ds = ds.rename_dims({dim_name: f'{dim_name}_{spatial_resolution}'
                             for dim_name in new_gm.xy_dim_names})
        ds = ds.rename_vars({var_name: f'{var_name}_{spatial_resolution}'
                             for var_name in new_gm.xy_var_names})
        if 'crs' in ds:
            ds = ds.rename_vars({'crs': f'crs_{spatial_resolution}'})
    else:
        if 'x' in ds_target.dims and 'y' in ds_target.dims:
            ds = ds.assign(x=ds_target.x)
            ds = ds.assign(y=ds_target.y)
        elif 'lat' in ds_target.dims and 'lon' in ds_target.dims:
            ds = ds.assign(lat=ds_target.lat)
            ds = ds.assign(lon=ds_target.lon)
    return ds


def _resample_temporally(ds_source: xr.Dataset, ds_target: xr.Dataset,
                         time_range, time_period:str, resampling_method: str) \
        -> xr.Dataset:
    ds = ds_source.sel(time=slice(time_range[0], time_range[1]))
    resampler = ds.resample(skipna=True, time=time_period)
    if resampling_method == 'max':
        ds = resampler.max()
    if resampling_method == 'min':
        ds = resampler.min()
    if resampling_method == 'mean':
        ds = resampler.mean()
    ds = ds.assign(time=ds_target.time)
    return ds


def _subset_spatially_around_center(ds: xr.Dataset,
                                    center_lat: float,
                                    center_lon: float,
                                    num_degrees: int = 1) -> xr.Dataset:
    min_lat = center_lat - num_degrees
    max_lat = center_lat + num_degrees
    min_lon = center_lon - num_degrees
    max_lon = center_lon + num_degrees
    if ds.lat.values[0] > ds.lat.values[-1]:
        return ds.sel(lat=slice(max_lat, min_lat), lon=slice(min_lon, max_lon))
    else:
        return ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))


def _subset_temporally(ds_source: xr.Dataset, time_range) \
        -> xr.Dataset:
    return ds_source.sel(time=slice(time_range[0], time_range[1]))


if __name__ == "__main__":
    if len(sys.argv) > 4:
        raise ValueError('Too many arguments')
    geojson_file = sys.argv[1]
    aws_access_key_id = sys.argv[2]
    aws_secret_access_key = sys.argv[3]
    with open(geojson_file, 'r') as gjf:
        generate_cube(
            json.load(gjf),
            aws_access_key_id,
            aws_secret_access_key
        )
