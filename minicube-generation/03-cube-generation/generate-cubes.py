import glob
import subprocess
import sys


if __name__ == "__main__":
    if len(sys.argv) > 4:
        raise ValueError('Too many arguments')
    geojson_location = sys.argv[1]
    if geojson_location.endswith('*'):
        geojson_files = glob.glob(geojson_location)
    else:
        geojson_files = [geojson_location]
    aws_access_key_id = sys.argv[2]
    aws_secret_access_key = sys.argv[3]
    for geojson_file in geojson_files:
        subprocess.run(['python', 'generate-cube.py', f'{geojson_file}',
                        f'{aws_access_key_id}', f'{aws_secret_access_key}'])
