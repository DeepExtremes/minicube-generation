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
    if len(sys.argv) > 3:
        raise ValueError('Too many arguments')
    geojson_location = sys.argv[1]
    if geojson_location.endswith('.geojson'):
        geojson_files = [geojson_location]
    else:
        fs = get_fs()
        geojson_files = fs.ls(geojson_location)
        remove_items = []
        for geojson_file in geojson_files:
            if not geojson_file.endswith('.geojson') or '/created/' in geojson_file:
                remove_items.append(geojson_file)
        for remove_item in remove_items:
            geojson_files.remove(remove_item)
    running_processes = dict()
    not_start_count = 0
    start_more_count = 0
    for i, geojson_file in enumerate(geojson_files):
        command = ['python', 'generate-cube.py', f'{geojson_file}']
        if len(sys.argv) == 3:
            processes_to_remove = []
            for process_name, process in running_processes.items():
                if process.poll() is not None:
                    processes_to_remove.append(process_name)
            for process in processes_to_remove:
                running_processes.pop(process)
            num_running_processes = len(running_processes.items())
            if num_running_processes < int(sys.argv[2]):
                print(f'Only {num_running_processes} running, '
                      f'will start one more ({start_more_count})')
                running_processes[geojson_file] = subprocess.Popen(command)
                start_more_count += 1
                not_start_count = 0
            else:
                print(f'Already {num_running_processes} running, '
                      f'will not start more right now ({not_start_count})')
                not_start_count += 1
                start_more_count = 0
            time.sleep(60)
        else:
            subprocess.run(command)
