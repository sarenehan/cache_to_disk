# Standard Library
import os
import pickle
import json
from datetime import datetime
from os.path import getmtime, isfile, join

max_bytes = 2**31 - 1
disk_cache_dir = os.path.dirname(os.path.realpath(__file__)) + '/disk_cache/'


def write_cache_file(cache_metadata_dict):
    with open(disk_cache_dir + 'cache_to_disk_caches.json', 'w') as f:
        return json.dump(cache_metadata_dict, f)


def load_cache_metadata_json():
    try:
        with open(disk_cache_dir + 'cache_to_disk_caches.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        write_cache_file({'total_number_of_cache_to_disks': 0})
        return {'total_number_of_cache_to_disks': 0}


def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        write_cache_file({'total_number_of_cache_to_disks': 0})


def pickle_big_data(data, file_path):
    bytes_out = pickle.dumps(data, protocol=4)
    with open(file_path, 'wb') as f_out:
        for idx in range(0, len(bytes_out), max_bytes):
            f_out.write(bytes_out[idx:idx + max_bytes])


def unpickle_big_data(file_path):
    try:
        with open(file_path, 'rb') as f:
            return pickle.load(f)
    except Exception:
        bytes_in = bytearray(0)
        input_size = os.path.getsize(file_path)
        with open(file_path, 'rb') as f_in:
            for _ in range(0, input_size, max_bytes):
                bytes_in += f_in.read(max_bytes)
        return pickle.loads(bytes_in)


def get_age_of_file(filename, unit='days'):
    age = (datetime.today() - datetime.fromtimestamp(getmtime(filename)))
    return getattr(age, unit)


def get_files_in_directory(directory):
    return [
        f for f in os.listdir(directory) if
        isfile(join(directory, f))
    ]


def delete_old_disk_caches():
    cache_metadata = load_cache_metadata_json()
    for function_name, function_caches in cache_metadata.items():
        if function_name != 'total_number_of_cache_to_disks':
            for function_cache in function_caches:
                max_age_days = int(function_cache['max_age_days'])
                file_name = disk_cache_dir + function_cache['file_name']
                if get_age_of_file(file_name) > max_age_days:
                    os.remove(file_name)


def delete_disk_caches_for_function(function_name):
    n_deleted = 0
    cache_metadata = load_cache_metadata_json()
    if function_name not in cache_metadata:
        print('Removed {} caches for {}'.format(n_deleted, function_name))
        return
    functions_to_delete_cache_for = cache_metadata.pop(function_name)
    for function_cache in functions_to_delete_cache_for:
        file_name = disk_cache_dir + function_cache['file_name']
        os.remove(file_name)
        n_deleted += 1
    print('Removed {} caches for {}'.format(n_deleted, function_name))
    write_cache_file(cache_metadata)


def cache_exists(cache_metadata, function_name, *args, **kwargs):
    if function_name not in cache_metadata:
        return False, None
    new_caches_for_function = []
    cache_changed = False
    for function_cache in cache_metadata[function_name]:
        if function_cache['args'] == str(args) and (
                function_cache['kwargs'] == str(kwargs)):
            file_name = disk_cache_dir + function_cache['file_name']
            max_age_days = int(function_cache['max_age_days'])
            file_name = disk_cache_dir + function_cache['file_name']
            if get_age_of_file(file_name) > max_age_days:
                os.remove(file_name)
                cache_changed = True
            else:
                function_value = unpickle_big_data(file_name)
                return True, function_value
        else:
            new_caches_for_function.append(function_cache)
    if cache_changed:
        if len(new_caches_for_function):
            cache_metadata[function_name] = new_caches_for_function
        else:
            cache_metadata.pop(function_name)
        write_cache_file(cache_metadata)
    return False, None


def cache_function_value(
        function_value,
        n_days_to_cache,
        cache_metadata,
        function_name,
        *args,
        **kwargs):
    if function_name == 'total_number_of_cache_to_disks':
        raise Exception(
            'Cant cache function named total_number_of_cache_to_disks')
    function_caches = cache_metadata.get(function_name, [])
    new_file_name = str(int(cache_metadata[
        'total_number_of_cache_to_disks']) + 1) + '.pkl'
    new_cache = {
        'args': str(args),
        'kwargs': str(kwargs),
        'file_name': new_file_name,
        'max_age_days': n_days_to_cache
    }
    pickle_big_data(function_value, disk_cache_dir + new_file_name)
    function_caches.append(new_cache)
    cache_metadata[function_name] = function_caches
    cache_metadata['total_number_of_cache_to_disks'] = \
        int(cache_metadata['total_number_of_cache_to_disks']) + 1
    write_cache_file(cache_metadata)


def cache_to_disk(n_days_to_cache):
    def decorator(original_func):
        delete_old_disk_caches()

        def new_func(*args, **kwargs):
            cache_metadata = load_cache_metadata_json()
            already_cached, function_value = cache_exists(
                    cache_metadata, original_func.__name__, *args, **kwargs)
            if already_cached:
                return function_value
            function_value = original_func(*args, **kwargs)
            cache_function_value(
                function_value,
                n_days_to_cache,
                cache_metadata,
                original_func.__name__,
                *args,
                **kwargs)
            return function_value
        return new_func
    return decorator


ensure_dir(disk_cache_dir)
delete_old_disk_caches()
