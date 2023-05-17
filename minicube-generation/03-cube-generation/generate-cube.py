from datetime import datetime
import geopandas as gpd
import json
import fsspec
import os
import pandas as pd
import rioxarray
from shapely.geometry import Polygon
import sys
import time
import xarray as xr
from typing import Dict

from xcube.core.gridmapping import GridMapping
from xcube.core.store import new_data_store
from xcube.core.mldataset import MultiLevelDataset
from xcube.core.resampling import resample_in_space
from xcube.core.update import update_dataset_chunk_encoding

from maskaycloudmask import compute_cloud_mask

_MC_REGISTRY = 'deepextremes-minicubes/mc_registry_v2.csv'
_MONTHS = dict(
    jan=1, feb=2, mar=3, apr=4, may=5, jun=6,
    jul=7, aug=8, sep=9, oct=10, nov=11, dec=12
)
_S2_L2_SCHEMA = \
    {'type': 'object',
     'properties':
         {'variable_names':
              {'type': ['array', 'null'],
               'items': {'type': 'string'}
               },
          'variable_fill_values':
              {'type': 'array',
               'items':
                   {'type': ['number', 'null']
                    }
               },
          'variable_sample_types':
              {'type': 'array',
               'items': {'type': 'string'}
               },
          'variable_units':
              {'type': 'array',
               'items':
                   {'type': 'string'}
               },
          'tile_size':
              {'type': 'array',
               'default': (1000, 1000),
               'items':
                   [
                       {'type': 'number',
                        'default': 1000,
                        'minimum': 1,
                        'maximum': 2500},
                       {'type': 'number',
                        'default': 1000,
                        'minimum': 1,
                        'maximum': 2500}
                   ]
               },
          'crs':
              {'type': 'string',
               'default': 'WGS84',
               'enum': ['CRS84', 'WGS84', 'EPSG:4326', 'EPSG:3857', 'EPSG:2154', 'EPSG:2180', 'EPSG:2193', 'EPSG:3003',
                        'EPSG:3004', 'EPSG:3031', 'EPSG:3035', 'EPSG:3346', 'EPSG:3416', 'EPSG:3765', 'EPSG:3794',
                        'EPSG:3844', 'EPSG:3912', 'EPSG:3995', 'EPSG:4026', 'EPSG:5514', 'EPSG:28992', 'EPSG:32601',
                        'EPSG:32602', 'EPSG:32603', 'EPSG:32604', 'EPSG:32605', 'EPSG:32606', 'EPSG:32607',
                        'EPSG:32608', 'EPSG:32609', 'EPSG:32610', 'EPSG:32611', 'EPSG:32612', 'EPSG:32613',
                        'EPSG:32614', 'EPSG:32615', 'EPSG:32616', 'EPSG:32617', 'EPSG:32618', 'EPSG:32619',
                        'EPSG:32620', 'EPSG:32621', 'EPSG:32622', 'EPSG:32623', 'EPSG:32624', 'EPSG:32625',
                        'EPSG:32626', 'EPSG:32627', 'EPSG:32628', 'EPSG:32629', 'EPSG:32630', 'EPSG:32631',
                        'EPSG:32632', 'EPSG:32633', 'EPSG:32634', 'EPSG:32635', 'EPSG:32636', 'EPSG:32637',
                        'EPSG:32638', 'EPSG:32639', 'EPSG:32640', 'EPSG:32641', 'EPSG:32642', 'EPSG:32643',
                        'EPSG:32644', 'EPSG:32645', 'EPSG:32646', 'EPSG:32647', 'EPSG:32648', 'EPSG:32649',
                        'EPSG:32650', 'EPSG:32651', 'EPSG:32652', 'EPSG:32653', 'EPSG:32654', 'EPSG:32655',
                        'EPSG:32656', 'EPSG:32657', 'EPSG:32658', 'EPSG:32659', 'EPSG:32660', 'EPSG:32701',
                        'EPSG:32702', 'EPSG:32703', 'EPSG:32704', 'EPSG:32705', 'EPSG:32706', 'EPSG:32707',
                        'EPSG:32708', 'EPSG:32709', 'EPSG:32710', 'EPSG:32711', 'EPSG:32712', 'EPSG:32713',
                        'EPSG:32714', 'EPSG:32715', 'EPSG:32716', 'EPSG:32717', 'EPSG:32718', 'EPSG:32719',
                        'EPSG:32720', 'EPSG:32721', 'EPSG:32722', 'EPSG:32723', 'EPSG:32724', 'EPSG:32725',
                        'EPSG:32726', 'EPSG:32727', 'EPSG:32728', 'EPSG:32729', 'EPSG:32730', 'EPSG:32731',
                        'EPSG:32732', 'EPSG:32733', 'EPSG:32734', 'EPSG:32735', 'EPSG:32736', 'EPSG:32737',
                        'EPSG:32738', 'EPSG:32739', 'EPSG:32740', 'EPSG:32741', 'EPSG:32742', 'EPSG:32743',
                        'EPSG:32744', 'EPSG:32745', 'EPSG:32746', 'EPSG:32747', 'EPSG:32748', 'EPSG:32749',
                        'EPSG:32750', 'EPSG:32751', 'EPSG:32752', 'EPSG:32753', 'EPSG:32754', 'EPSG:32755',
                        'EPSG:32756', 'EPSG:32757', 'EPSG:32758', 'EPSG:32759', 'EPSG:32760']
               },
          'bbox':
              {'type': 'array',
               'items': [
                   {'type': 'number'},
                   {'type': 'number'},
                   {'type': 'number'},
                   {'type': 'number'}
               ]},
          'spatial_res':
              {'type': 'number', 'exclusiveMinimum': 0.0},
          'upsampling': {'type': 'string', 'default': 'NEAREST', 'enum': ['NEAREST', 'BILINEAR', 'BICUBIC']},
          'downsampling': {'type': 'string', 'default': 'NEAREST', 'enum': ['NEAREST', 'BILINEAR', 'BICUBIC']},
          'mosaicking_order': {'type': 'string', 'default': 'mostRecent',
                               'enum': ['mostRecent', 'leastRecent', 'leastCC']},
          'time_range': {
              'type': ['array', 'null'],
              'items': [{'type': ['string', 'null'],
                         'format': 'date', 'minDate': '2016-11-01'},
                        {'type': ['string', 'null'],
                         'format': 'date', 'minDate': '2016-11-01'}
                        ]
          },
          'time_period':
              {'type': ['string', 'null'],
               'default': '1D',
               'enum':
                   [None, '1D', '2D', '3D', '4D', '5D', '6D', '7D', '8D', '9D', '10D', '11D', '12D', '13D', '1W', '2W']
               },
          'time_tolerance':
              {'type': 'string', 'default': '10m', 'format': '^([1-9]*[0-9]*)[NULSTH]$'},
          'collection_id': {'type': 'string'},
          'four_d': {'type': 'boolean', 'default': False},
          'max_cache_size': {'type': 'integer', 'minimum': 0}
          },
     'required': ['bbox', 'spatial_res', 'time_range']
     }


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
            if dataset_name == 'S2L2A':
                open_properties = _S2_L2_SCHEMA.get('properties')
            else:
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
        split_data_id = mc_config['properties']['data_id'].split('_')
        center_lon = split_data_id[1]
        center_lat = split_data_id[2]
        return _subset_spatially_around_center(
            ds=ps_ds,
            center_lat=float(center_lat),
            center_lon=float(center_lon),
            num_degrees=int(params_string) if params_string else None
        )
    if processing_step.startswith('Pick /'):
        return _pick(ps_ds, ending=params_string)
    if processing_step == 'Pick center spatial value':
        split_data_id = mc_config['properties']['data_id'].split('_')
        center_lon = split_data_id[1]
        center_lat = split_data_id[2]
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
    if processing_step.startswith('Rechunk by to /'):
        dim_name, chunk_size = params_string.split(' ')
        return _rechunk(
            ds_source=ps_ds,
            dim_name=dim_name,
            chunk_size=int(chunk_size)
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
    data_id = mc_config['properties']['data_id']
    if not mc_config['properties'].get('location_id', None):
        mc_config['properties']['location_id'] = \
            '_'.join(data_id.split('_')[1:3])
    variable_properties = mc_config['properties'].pop('variables')
    mc_config['properties'].pop('sources')
    vc_dict = {vc['name']: vc for vc in variable_properties}
    date_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if mc_config.get('config_type', 'base') == 'base':
        mc_config['properties']['metadata']['history'] = \
            [f'Created at {date_now} as {data_id}']
        mc_config['properties']['creation_date'] = date_now
    else:
        mc_config['properties']['metadata'].get('history', []).append(
            f'Modified at {date_now} as {data_id}'
        )
        mc_config['properties']['modification_date'] = date_now
    ds = ds.assign_attrs(**mc_config['properties'])
    for variable_name, attrs in vc_dict.items():
        ds[variable_name] = ds[variable_name].assign_attrs(attrs)
    return ds


def _get_encoding_dict(ds: xr.Dataset) -> dict:
    encodings = {}
    for data_var in ds.data_vars:
        if 'encoding' in ds[data_var].attrs:
            encodings[data_var] = ds[data_var].attrs.get('encoding')
    return encodings


def _get_s3_store(version: str):
    s3_key = os.environ["S3_USER_STORAGE_KEY"]
    s3_secret = os.environ["S3_USER_STORAGE_SECRET"]
    root = f'deepextremes-minicubes/{version}' \
        if version else "deepextremes-minicubes"
    return new_data_store(
        "s3",
        root=root,
        storage_options=dict(
            anon=False,
            key=s3_key,
            secret=s3_secret
        )
    )


def _write_ds(ds: xr.Dataset, mc_config: dict):
    print(f'Writing dataset {ds.data_id}')
    s3_store = _get_s3_store(mc_config['properties']['version'])
    encodings = _get_encoding_dict(ds)
    s3_store.write_data(
        ds, f"{ds.data_id}.zarr", encoding=encodings
    )
    # enable to write local
    # ds.to_zarr(f"{version}/{ds.data_id}.zarr")
    print(f'Finished writing dataset {ds.data_id}')


def _get_gdf_from_mc(mc: xr.Dataset) -> gpd.GeoDataFrame:
    mc_id = mc.attrs.get('data_id')
    location_id = mc.attrs.get('location_id')
    version = mc.attrs.get('version')
    path = f'deepextremes-minicubes/{version}/{mc_id}.zarr'
    creation_date = mc.attrs.get('creation_date')
    modification_date = mc.attrs.get('modification_date', creation_date)
    events = str([(
        mc.attrs.get('metadata', {}).get('event_label'),
        mc.attrs.get('metadata', {}).get('event_start_time'),
        mc.attrs.get('metadata', {}).get('event_end_time')
    )])

    lon_min = mc.attrs.get('metadata', {}).get('geospatial_lon_min')
    lon_max = mc.attrs.get('metadata', {}).get('geospatial_lon_max')
    lat_min = mc.attrs.get('metadata', {}).get('geospatial_lat_min')
    lat_max = mc.attrs.get('metadata', {}).get('geospatial_lat_max')

    geometry = Polygon([[lon_min, lat_min], [lon_max, lat_min],
                        [lon_max, lat_max], [lon_min, lat_max]])
    remarks = 'no climatology' if version.endswith('.n') else ''

    configuration_versions = \
        mc.attrs.get('metadata', {}).get('configuration_versions', {})
    s2_l2_bands_version = configuration_versions.get('s2_l2_bands', -1)
    era5_version = configuration_versions.get('era5', -1)
    cci_landcover_map_version = \
        configuration_versions.get('cci_landcover_map', -1)
    copernicus_dem_version = configuration_versions.get('copernicus_dem', -1)
    de_africa_climatology_version = \
        configuration_versions.get('de_africa_climatology', -1)
    event_arrays_version = configuration_versions.get('event_arrays', -1)
    s2cloudless_cloudmask_version = \
        configuration_versions.get('s2cloudless_cloudmask', -1)
    sen2cor_cloudmask_version = \
        configuration_versions.get('sen2cor_cloudmask', -1)
    unetmobv2_cloudmask_version = \
        configuration_versions.get('unetmobv2_cloudmask', -1)
    reg_gdf = gpd.GeoDataFrame(
        {
            'mc_id': mc_id,
            'path': path,
            'location_id': location_id,
            'version': version,
            'geometry': geometry,
            'creation_date': creation_date,
            'modification_date': modification_date,
            'events': events,
            's2_l2_bands': s2_l2_bands_version,
            'era5': era5_version,
            'cci_landcover_map': cci_landcover_map_version,
            'copernicus_dem': copernicus_dem_version,
            'de_africa_climatology': de_africa_climatology_version,
            'event_arrays': event_arrays_version,
            's2cloudless_cloudmask': s2cloudless_cloudmask_version,
            'sen2cor_cloudmask': sen2cor_cloudmask_version,
            'unetmobv2_cloudmask': unetmobv2_cloudmask_version,
            'remarks': remarks
        },
        index=[0]
    )
    return reg_gdf


def _get_minicubes_fs() -> fsspec.filesystem:
    s3_key = os.environ["S3_USER_STORAGE_KEY"]
    s3_secret = os.environ["S3_USER_STORAGE_SECRET"]
    storage_options = dict(
        anon=False,
        key=s3_key,
        secret=s3_secret
    )
    return fsspec.filesystem('s3', **storage_options)


def _already_registered(mc_config: dict) -> bool:
    fs = _get_minicubes_fs()
    location_id = mc_config["properties"]["location_id"]
    with fs.open(_MC_REGISTRY, 'r') as gjreg:
        gpdreg = gpd.GeoDataFrame(pd.read_csv(gjreg))
        if len(gpdreg.loc[gpdreg['location_id'] == location_id]) > 0:
            return True
    return False


def _remove_cube_if_exists(mc_config: dict):
    # check whether minicube exists. If it does, we assume there is something
    # wrong with it as there is no registry entry, so we will delete it
    fs = _get_minicubes_fs()
    version = mc_config['properties']['version']
    minicube_ids = fs.ls(f'deepextremes-minicubes/{version}/')
    location_ids = ['_'.join(minicube_id.split('_')[1:3])
                    for minicube_id in minicube_ids]
    mc_location_id = mc_config["properties"].get("location_id")
    if not mc_location_id:
        mc_data_id = mc_config["properties"]["data_id"]
        mc_location_id = '_'.join(mc_data_id.split('_')[1:3])
    if mc_location_id in location_ids:
        index = location_ids.index(mc_location_id)
        mc_id = minicube_ids[index]
        print(f'Cube {mc_id} has no entry, will remove')
        fs.delete(mc_id, recursive=True)


def _write_entry(ds: xr.Dataset, mc_config: dict):
    fs = _get_minicubes_fs()
    gdf = _get_gdf_from_mc(ds)
    not_written = True
    while not_written:
        try:
            open('.lock', 'x')
            if mc_config.get("config_type", "base") == 'base':
                with fs.open(_MC_REGISTRY, 'a') as registry:
                    registry.write(gdf.to_csv(header=False, index=False))
            else:
                with fs.open(_MC_REGISTRY, 'r') as registry:
                    gpdreg = gpd.GeoDataFrame(pd.read_csv(registry))
                mc_location_id = mc_config['properties']['location_id']
                gpdreg = gpdreg.drop(
                    gpdreg[gpdreg.location_id == mc_location_id].index
                )
                gpdreg = gpdreg.append(gdf)
                with fs.open(_MC_REGISTRY, 'w') as registry:
                    registry.write(gpdreg.to_csv(index=False))
            not_written = False
            os.remove('.lock')
        except FileExistsError:
            time.sleep(5)


def _open_base_mc(mc_config: dict):
    s3_store = _get_s3_store(mc_config.get("properties").get("version"))
    base_mc_id = mc_config["properties"].get("base_minicube")
    adjusted_base_mc_id = base_mc_id.split('/')[-1]
    if not s3_store.has_data(adjusted_base_mc_id):
        raise ValueError(f'Could not find base minicube {base_mc_id}. '
                         f'Update not possible.')
    return s3_store.open_data(adjusted_base_mc_id)


def _resample_version(base_mc: xr.Dataset) -> xr.Dataset:
    to_drop = [dv for dv in base_mc.data_vars if not dv.startswith('B0')]
    to_drop.remove('crs')
    return base_mc.drop_vars(to_drop)


def generate_cube(mc_config: dict,
                  aws_access_key_id: str,
                  aws_secret_access_key:str
                  ):
    print(f'Processing minicube configuration '
          f'{mc_config["properties"]["data_id"]}')
    update = mc_config.get("config_type", "base") == 'update'
    base_mc = _open_base_mc(mc_config) if update else None
    mc_to_be_merged = base_mc if update else None
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
                if update:
                    resampling_target = _resample_version(base_mc)
                else:
                    resampling_target = datasets[
                        processing_step.split('to ')[1].split(' /')[0]]
                additional_sources['resampling_target'] = resampling_target
            ps_ds = _execute_processing_step(processing_step, ps_ds,
                                             additional_sources, mc_config)
        if update:
            for data_var in ps_ds.data_vars:
                if data_var in mc_to_be_merged:
                    mc_to_be_merged = mc_to_be_merged.drop_vars([data_var])
        output_datasets.append(ps_ds)

    if update:
        output_datasets.append(mc_to_be_merged)

    ds = xr.merge(output_datasets)

    ds = _finalize_dataset(ds, mc_config)

    _write_ds(ds, mc_config)

    _write_entry(ds, mc_config)

    if mc_config.get("config_type", "base") == 'update':
        base_mc_id = mc_config["properties"].get("base_minicube")
        # remove leading bucket name
        adjusted_base_mc_id = base_mc_id.split('/')[-1]
        _get_s3_store(mc_config['properties']['version']).\
            delete_data(adjusted_base_mc_id)
        print("Deleted previous entry of cube")

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


def _rechunk(ds_source: xr.Dataset, dim_name: str, chunk_size:int) -> xr.Dataset:
    ds = ds_source
    for data_var in ds.data_vars:
        if dim_name in ds[data_var].dims:
            ds[data_var] = ds[data_var].chunk(chunks={dim_name: chunk_size})
            ds = update_dataset_chunk_encoding(ds,
                                               chunk_sizes={dim_name: chunk_size},
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
    if 'spatial_ref' in ds:
        ds = ds.drop_vars('spatial_ref')
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
    fs = _get_minicubes_fs()
    with fs.open(geojson_file, 'r') as gjf:
        mc_config = json.load(gjf)
        if mc_config.get("config_type", "base") == 'base':
            if _already_registered(mc_config):
                location_id = mc_config["properties"].get("location_id", "")
                print(f'Minicube at location {location_id} already exists, '
                      f'will not generate but move config to created folder')
            else:
                _remove_cube_if_exists(mc_config)
                generate_cube(
                    mc_config,
                    aws_access_key_id,
                    aws_secret_access_key
                )
        else:   # config_type == 'update'
            if not _already_registered(mc_config):
                location_id = mc_config["properties"]["location_id"]
                print(f'No minicube at location {location_id} found in entry, '
                      f'will not update but move config to created folder')
                _remove_cube_if_exists(mc_config)
            else:
                generate_cube(
                    mc_config,
                    aws_access_key_id,
                    aws_secret_access_key
                )
    split_geojson_file = geojson_file.split('/')
    split_geojson_file.insert(-1, 'created')
    new_file = '/'.join(split_geojson_file)
    fs.move(geojson_file, new_file)
