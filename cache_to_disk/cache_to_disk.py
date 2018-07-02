# Standard Library
from datetime import datetime
from os.path import isfile, join, exists, getmtime
import os
import pickle

# Thirdparty
import zlib

max_bytes = 2**31 - 1
disk_cache_dir = os.path.dirname(os.path.realpath(__file__)) + '/disk_cache/'


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
    n_deleted = 0
    deleted_caches = []
    for file in get_files_in_directory(disk_cache_dir):
        max_age_days = int(file.split('_')[-1].replace('.pkl', ''))
        if get_age_of_file(disk_cache_dir + file) > max_age_days:
            os.remove(disk_cache_dir + file)
            deleted_caches.append(file)
            n_deleted += 1
    print('Expired {} caches:'.format(n_deleted))
    for deleted_cache in deleted_caches:
        print('\t{}'.format(deleted_cache))


def delete_disk_caches_for_function(function_name):
    n_deleted = 0
    for file in get_files_in_directory(disk_cache_dir):
        cached_function = '_'.join(file.split('_')[1:-1])
        if function_name == cached_function:
            os.remove(disk_cache_dir + file)
            n_deleted += 1
    print('Removed {} caches for {}'.format(n_deleted, function_name))


def cache_to_disk(n_days_to_cache):
    def decorator(original_func):
        delete_old_disk_caches()

        def new_func(*args, **kwargs):
            prefix_str = original_func.__name__ + '::' + str(args) + str(
                kwargs)
            prefix = zlib.adler32(prefix_str.encode())
            filename = '{}_{}_{}.pkl'.format(
                prefix, original_func.__name__, n_days_to_cache)
            file_path = disk_cache_dir + filename
            if exists(file_path):
                return unpickle_big_data(file_path)
            function_value = original_func(*args, **kwargs)
            pickle_big_data(function_value, file_path)
            return function_value
        return new_func
    return decorator
