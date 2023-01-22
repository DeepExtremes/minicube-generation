import glob
import subprocess
import sys


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
        subprocess.run(['python', 'generate-cube.py', '{geojson_file}',
                        f'{client_id}', f'{client_secret}',
                        f'{aws_access_key_id}', f'{aws_secret_access_key}'])
