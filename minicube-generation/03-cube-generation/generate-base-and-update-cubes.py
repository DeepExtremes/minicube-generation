import enum
import subprocess
import sys
import time


class _Type(enum.Enum):
    BASE = 'base'
    UPDATE = 'update'


def _run_process(
        type: _Type, num_processes: int, version: str, running_processes: dict
) -> dict:
    running_processes[f'{type.value} {version}'] = \
        subprocess.Popen(['python',
                          'generate-cubes.py',
                          f'deepextremes-minicubes/configs/{type.value}/{version}',
                          num_processes
                          ])
    return running_processes


if __name__ == "__main__":
    if len(sys.argv) > 5:
        raise ValueError('Too many arguments')
    base_config_dirs = sys.argv[1].split(',')
    num_base_processes = int(sys.argv[2])
    update_config_dirs = sys.argv[3].split(',')
    num_update_processes = int(sys.argv[4])
    base_process_to_start = 0
    update_process_to_start = 0
    running_processes = dict()
    running_processes = _run_process(
        _Type.BASE, num_base_processes, base_config_dirs[base_process_to_start],
        running_processes
    )
    running_processes = _run_process(
        _Type.UPDATE, num_update_processes, update_config_dirs[update_process_to_start],
        running_processes
    )
    base_process_to_start += 1
    update_process_to_start += 1
    num_running_processes = len(running_processes.items())
    processes_have_been_shifted = False
    while num_running_processes > 0:
        time.sleep(60)
        processes_to_remove = []
        for process_name, process in running_processes.items():
            if process.poll() is not None:
                processes_to_remove.append(process_name)
        for process in processes_to_remove:
            running_processes.pop(process)
            if process.startswith('base') and \
                    base_process_to_start < len(base_config_dirs):
                running_processes = _run_process(
                    _Type.BASE, num_base_processes,
                    base_config_dirs[base_process_to_start],
                    running_processes
                )
                base_process_to_start += 1
            elif process.startswith('update') and \
                    update_process_to_start < len(update_config_dirs):
                running_processes = _run_process(
                    _Type.UPDATE, num_update_processes,
                    update_config_dirs[update_process_to_start],
                    running_processes
                )
                update_process_to_start += 1
        num_running_processes = len(running_processes.items())
        if num_running_processes == 1 and not processes_have_been_shifted:
            # Either no more base or no more update configs are available
            # Shift processes to the other type
            key = list(running_processes.keys())[0]
            if key.startswith('base'):
                running_processes[key].kill()
                time.sleep(60)
                num_base_processes += num_update_processes
                running_processes = _run_process(
                    _Type.BASE, num_base_processes,
                    base_config_dirs[base_process_to_start - 1],
                    running_processes
                )
            elif key.startswith('update'):
                running_processes[key].kill()
                time.sleep(60)
                num_update_processes += num_base_processes
                running_processes = _run_process(
                    _Type.UPDATE, num_update_processes,
                    update_config_dirs[update_process_to_start - 1],
                    running_processes
                )
            processes_have_been_shifted = True
        if num_running_processes == 1:
            key = list(running_processes.keys())[0]
            print(f'Running process "{key}"')
        elif num_running_processes == 2:
            key_1 = list(running_processes.keys())[0]
            key_2 = list(running_processes.keys())[1]
            print(f'Running processes "{key_1}" and "{key_2}"')
    subprocess.run(['kill'])
