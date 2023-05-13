import fsspec
import subprocess
import os
import sys
import time


def get_fs():
    s3_key = os.environ["S3_USER_STORAGE_KEY"]
    s3_secret = os.environ["S3_USER_STORAGE_SECRET"]
    storage_options = dict(
        anon=False,
        key=s3_key,
        secret=s3_secret
    )
    return fsspec.filesystem('s3', **storage_options)


if __name__ == "__main__":
    if len(sys.argv) > 5:
        raise ValueError('Too many arguments')
    geojson_location = sys.argv[1]
    if geojson_location.endswith('.geojson'):
        geojson_files = [geojson_location]
    else:
        fs = get_fs()
        geojson_files = fs.ls(geojson_location)
        for geojson_file in geojson_files:
            if not geojson_file.endswith('.geojson'):
                geojson_files.remove(geojson_file)
    aws_access_key_id = sys.argv[2]
    aws_secret_access_key = sys.argv[3]
    for geojson_file in geojson_files:
        command = ['python', 'generate-cube.py', f'{geojson_file}',
                 f'{aws_access_key_id}', f'{aws_secret_access_key}']
        if len(sys.argv) == 5:
            seconds_to_sleep = int(sys.argv[4]) * 60
            subprocess.Popen(command)
            time.sleep(seconds_to_sleep)
        else:
            subprocess.run(command)
