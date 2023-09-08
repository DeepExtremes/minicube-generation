import geopandas as gpd
import pandas as pd
import sys
from typing import List

from configgencreation import get_available_components
from configgencreation import get_fs
from configgencreation import get_list_of_csv_location_ids
from configgencreation import get_store
from configgencreation import create_update_config


from constants import MC_REGISTRY


def _update_components(base_version: str, components_to_update: List[str]):
    mc_base_store = get_store(f'deepextremes-minicubes/base/{base_version}/')
    available_components = get_available_components()
    for component in components_to_update:
        if component not in available_components:
            raise ValueError(f'Component {component} was not found. '
                             f'Must be one of {available_components}')
    csv_location_ids = get_list_of_csv_location_ids()
    fs = get_fs()
    with fs.open(MC_REGISTRY, 'r') as gjreg:
        gpdreg = gpd.GeoDataFrame(pd.read_csv(gjreg))
        entries = gpdreg.loc[gpdreg.version == base_version]
        for i, row in entries.iterrows():
            minicube_id = row.mc_id
            mc_path = row.path
            mc = mc_base_store.open_data(f'{minicube_id}.zarr')
            create_update_config(
                mc, mc_path, components_to_update, csv_location_ids
            )


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise ValueError(f'This function requires a path to a bucket containing '
                         f'minicubes and a list of components which shall be updated. '
                         f'Available: components: {get_available_components()}')
    base_version = sys.argv[1]
    components_to_update = sys.argv[2:]
    _update_components(base_version, components_to_update)
