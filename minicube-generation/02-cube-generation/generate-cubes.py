import json
import glob
import sys
import xarray as xr
from typing import Dict
from typing import List

from xcube.core.store import DataStore
from xcube.core.store import new_data_store


def _set_up_sources(source_configs: List[dict]) -> Dict[str, DataStore]:
    sources = {}
    for source_config in source_configs:
        if 'store_id' in source_config:
            sources[source_config['name']] = new_data_store(
                source_config['store_id'],
                **source_config.get('store_params', {})
            )
    return sources


def get_source_dataset(source: DataStore, mc_config: dict) -> xr.Dataset:
    variable_names = mc_config['properties']



def generate_cube(mc_config: dict):
    # set up sources
    sources = mc_config['properties']['sources']
    # sources = _set_up_sources(mc_config['properties']['sources'])

    for source in sources:



    # for source_name, source in sources.items():


        # read inputs
        source_dataset = get_source_dataset(source, mc_config)

        # process

        # add to cube

    # write cube


if __name__ == "__main__":
    if len(sys.argv) > 1:
        geojson_files = sys.argv[1:]
    else:
        geojson_files = glob.glob('../configs/*.geojson')
    for geojson_file in geojson_files:
        with open(geojson_file, 'r') as gjf:
            generate_cube(json.load(gjf))