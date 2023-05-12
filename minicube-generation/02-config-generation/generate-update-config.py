import sys
from typing import List

from config-gen-creation import get_available_components
from config-gen-creation import get_store
from config-gen-creation import create_update_config


def _update_components(mc_path: str, components_to_update: List[str]):
    split_path = mc_path.split('/')
    minicube_id = split_path[-1]
    mc_bucket = '/'.join(split_path)
    mc_store = get_store(mc_bucket)
    available_components = get_available_components()
    for component in components_to_update:
        if component not in available_components:
            raise ValueError(f'Component {component} was not found. Must be one of '
                             f'{available_components}')
    mc = mc_store.open_data(minicube_id)
    create_update_config(mc, components_to_update)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise ValueError(f'This function requires a path to a minicube in a bucket '
                         f'and a list of components which shall be updated. '
                         f'Available: components: {get_available_components()}')
    mc_path = sys.argv[1]
    components_to_update = sys.argv[2:]
    _update_components(mc_path, components_to_update)
