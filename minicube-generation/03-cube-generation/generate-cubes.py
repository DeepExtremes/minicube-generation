import json
import glob
import os
import sys
import xarray as xr
from typing import Dict
from typing import List

from xcube.core.store import DataStore
from xcube.core.store import new_data_store


def _set_up_sources(source_configs: List[dict],
                    client_id:str,
                    client_secret:str) -> Dict[str, DataStore]:
    sources = {}
    for source_config in source_configs:
        if 'store_id' in source_config:
            if source_config['store_id'] == 'sentinelhub':
                store_params = source_config.get('store_params', {})
                store_params['client_id'] = client_id
                store_params['client_secret'] = client_secret
                source_config['store_params'] = store_params
            sources[source_config['name']] = new_data_store(
                source_config['store_id'],
                **source_config.get('store_params', {})
            )
    return sources


def _get_source_datasets(source_config: dict,
                         client_id: str,
                         client_secret: str,
                         mc_config: dict) -> Dict[str, xr.Dataset]:
    datasets = {}
    if 'store_id' in source_config:
        if source_config['store_id'] == 'sentinelhub':
            store_params = source_config.get('store_params', {})
            store_params['client_id'] = client_id
            store_params['client_secret'] = client_secret
            source_config['store_params'] = store_params
        source_store = new_data_store(
            source_config['store_id'],
            **source_config.get('store_params', {})
        )
        for dataset_name, variable_names in source_config['datasets'].items():
            datasets[dataset_name] = source_store.open_data(
                dataset_name,
                variable_names=variable_names['variable_names'],
                bbox=mc_config['properties']['spatial_bbox'],
                time_range=mc_config['properties']['time_range'],
                time_period=mc_config['properties']['time_period'],
                crs=mc_config['properties']['spatial_ref'],
                spatial_res=mc_config['properties']['spatial_res'],
            )
    return datasets


def _execute_processing_step(processing_step: str, ps_ds: xr.Dataset):
    # TODO: Add processing step implementations as necessary
    return ps_ds


def _finalize_dataset(ds: xr.Dataset, mc_config: dict) -> xr.Dataset:
    variable_properties = mc_config['properties'].pop('variables')
    mc_config['properties'].pop('sources')
    vc_dict = {vc['name']: vc for vc in variable_properties}
    ds = ds.assign_attrs(**mc_config['properties'])
    for variable_name, attrs in vc_dict.items():
        ds[variable_name] = ds[variable_name].assign_attrs(attrs)
    return ds


def _write_ds(ds: xr.Dataset,
              aws_access_key_id: str,
              aws_secret_access_key: str) -> xr.Dataset:
    s3_store = new_data_store(
        "s3",
        root="deepextremes",
        storage_options=dict(
            anon=False,
            client_kwargs=dict(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
        )
    )
    print(f'Writing dataset {ds.data_id}')
    s3_store.write_data(ds, f"minicubes/{ds.data_id}.zarr")
    print(f'Finished writing dataset {ds.data_id}')


def generate_cube(mc_config: dict, client_id: str, client_secret: str,
                  aws_access_key_id: str, aws_secret_access_key:str):
    variable_configs = mc_config['properties']['variables']
    processing_steps_set = set()
    for vc in variable_configs:
        ps = ";".join(vc.get('sources', [])[0].get('processing_steps', []))
        processing_steps_set.add(ps)

    sources = mc_config['properties']['sources']
    datasets = {}
    for source in sources:
        datasets.update(
            _get_source_datasets(
                source, client_id,client_secret , mc_config
            )
        )

    output_datasets = []
    for processing_steps_s in processing_steps_set:
        processing_steps = processing_steps_s.split(';')
        ps1_ds_name = processing_steps[0].split('Read ')[1]
        if ps1_ds_name not in datasets:
            raise ValueError(f'Invalid processing step: '
                             f'Dataset {ps1_ds_name} expected, '
                             f'but was not found in sources')
        ps_ds = datasets[ps1_ds_name]
        for processing_step in processing_steps[1:]:
            ps_ds = _execute_processing_step(processing_step, ps_ds)
        output_datasets.append(ps_ds)

    ds = xr.merge(output_datasets)

    ds = _finalize_dataset(ds, mc_config)

    _write_ds(ds, aws_access_key_id, aws_secret_access_key)


if __name__ == "__main__":
    if len(sys.argv) > 6:
        raise ValueError('Too many arguments')
    geojson_location = sys.argv[1]
    if geojson_location.endswith('*'):
        geojson_files = glob.glob(geojson_location)
    else:
        geojson_files = [geojson_location]
    client_id = sys.argv[2]
    client_secret = sys.argv[3]
    aws_access_key_id = sys.argv[4]
    aws_secret_access_key = sys.argv[5]
    for geojson_file in geojson_files:
        with open(geojson_file, 'r') as gjf:
            generate_cube(json.load(gjf),
                          client_id,
                          client_secret,
                          aws_access_key_id,
                          aws_secret_access_key
                          )