import sys
from typing import List

from configgencreation import get_available_components
from configgencreation import get_store
from configgencreation import create_update_config


def _update_components(mc_bucket: str, components_to_update: List[str]):
    mc_store = get_store(mc_bucket)
    minicube_ids = mc_store.list_data_ids()
    available_components = get_available_components()
    for component in components_to_update:
        if component not in available_components:
            raise ValueError(f'Component {component} was not found. Must be one of '
                             f'{available_components}')
    for minicube_id in minicube_ids:
        mc = mc_store.open_data(minicube_id)
        mc_path = f'{mc_bucket}/{minicube_id}'
        create_update_config(mc, mc_path, components_to_update)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise ValueError(f'This function requires a path to a bucket containing '
                         f'minicubes and a list of components which shall be updated. '
                         f'Available: components: {get_available_components()}')
    mc_bucket = sys.argv[1]
    components_to_update = sys.argv[2:]
    _update_components(mc_bucket, components_to_update)
