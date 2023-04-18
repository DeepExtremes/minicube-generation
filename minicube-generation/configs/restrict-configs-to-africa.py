import glob
import json
import os.path
import sys


def restrict_configs_to_africa(path_to_configs: str):
    config_files = glob.glob(f'{path_to_configs}/*')
    to_be_removed = []
    for config_file in config_files:
        with open(config_file, 'r') as cf:
            config = json.load(cf)
            source_names = [source['name']
                            for source in config['properties']['sources']]
            if 'NDVI Climatology' not in source_names:
                to_be_removed.append(config_file)
    for r in to_be_removed:
        os.remove(r)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError('Reguire path_to_configs')
    else:
        path_to_configs = sys.argv[1]
        restrict_configs_to_africa(path_to_configs)
