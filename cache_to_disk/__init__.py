"""cache_to_disk: Cache the results of functions persistently on disk

Original Work, Copyright (c) 2018 Stewart Renehan, MIT License
    Author: https://github.com/sarenehan
    Project: https://github.com/sarenehan/cache_to_disk

Modifications:
    Author: https://github.com/mzpqnxow
    Project: https://github.com/mzpqnxow/cache_to_disk/tree/feature/nocache

    This modified version adds the following:
        - Accounting of hits, misses and nocache events
        - cache_info(), cache_clear(), cache_size(), cache_get_raw() interfaces accessible
          via the function itself for convenience
        - NoCacheCondition exception, simple interface for a user to prevent a
          specific function result to not be cached, while still passing a return
          value to the caller
        - Minor refactoring of the decorator, for easier reading
        - Minor refactoring of delete_old_disk_caches(), to reduce logical blocks
          and depth of indentation
        - Default cache age value (DEFAULT_CACHE_AGE)
        - Special unlimited age value (UNLIMITED_CACHE_AGE)
        - Use of logging module (but defaulting to NullAdapter)
        - Minor PEP8 / cosmetic changes
        - Minor cosmetic changes to file path generation (use of os.path.join, a constant
          for the directory/file path)
        - Support getting cache directory or filename from environment:
            Cache metadata filename: $DISK_CACHE_FILENAME
            Base directory for cache files: $DISK_CACHE_DIR
        - Expansion of shell variables and tilde-user values for directories/files
"""
# Standard Library
import json
import logging
import os
import pickle
import warnings
from collections import namedtuple
from copy import deepcopy
from datetime import datetime
from os import getenv
from os.path import (
    dirname,
    exists as file_exists,
    expanduser, expandvars, getmtime,
    isfile,
    join as join_path,
    realpath)


logger = logging.getLogger(__name__)

if logger.handlers is None:
    # Don't log unless user explicitly adds a handler
    logger.addHandler(logging.NullHandler())

MAX_PICKLE_BYTES = 2 ** 31 - 1
DISK_CACHE_DIR = expanduser(expandvars(
    getenv('DISK_CACHE_DIR', join_path(dirname(realpath(__file__)), 'disk_cache'))))
DISK_CACHE_FILE = expanduser(expandvars(join_path(
    DISK_CACHE_DIR, getenv('DISK_CACHE_FILENAME', 'cache_to_disk_caches.json'))))

# Specify 0 for cache age days to keep forever; not recommended for obvious reasons
UNLIMITED_CACHE_AGE = 0
DEFAULT_CACHE_AGE = 7

_TOTAL_NUMCACHE_KEY = 'total_number_of_cache_to_disks'

# Run-time cache data, stolen from Python functools.lru_cache implementation
# Events resulting in nocache are cache misses that complete, but instruct cache_to_disk to
# not store the result. Useful, for example, in a function that makes a network request and
# experiences a failure that is considered likely to be temporary. This is accomplished in
# the user function by raising NoCacheCondition
_CacheInfo = namedtuple('CacheInfo', ['hits', 'misses', 'nocache'])

# This is probably unnecessary ...
# logger.debug('cache_to_disk package loaded; using DISK_CACHE_DIR=%s',
#             os.path.relpath(DISK_CACHE_DIR, '.'))


class NoCacheCondition(Exception):
    """Custom exception for user function to raise to prevent caching on a per-call basis

    The function_value kwarg can be set as a kwarg to return a value other than None to the
    original caller

    Example
    -------
    The following contrived example will return a value to the caller but avoids it being
    cached. In this example, a socket exception is considered a failure, but there is some
    value in returning a partial response to the caller in cases such as SIGPIPE/EPIPE in
    the read loop

    On a socket exception, the function will effectively return either an empty bytes
    buffer or a bytes buffer with partial response data, depending on where the network
    exception occurred

    @cache_to_disk(7)
    def network_query(hostname, port, query):
        response = b''
        try:
            socket = tcp_connect(hostname)
            socket.send(query)
            while True:
                # Build the response incrementally
                buf = read_bytes(socket, 1024)
                if buf is None:
                    break
                response += buf
        except socket.error:
            raise NoCacheCondition(function_value=buf)

        return response
    """
    __slots__ = ['function_value']

    def __init__(self, function_value=None):
        self.function_value = function_value
        logger.info('NoCacheCondition caught in cache_to_disk')


def write_cache_file(cache_metadata_dict):
    """Dump an object as JSON to a file"""
    with open(DISK_CACHE_FILE, 'w') as f:
        return json.dump(cache_metadata_dict, f)


def load_cache_metadata_json():
    """Load a JSON file, create it with empty cache structure if it doesn't exist"""
    try:
        with open(DISK_CACHE_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        write_cache_file({_TOTAL_NUMCACHE_KEY: 0})
        return {_TOTAL_NUMCACHE_KEY: 0}


def ensure_dir(directory):
    """Create a directory tree if it doesn't already exist"""
    if not file_exists(directory):
        os.makedirs(directory)
        write_cache_file({_TOTAL_NUMCACHE_KEY: 0})


def pickle_big_data(data, file_path):
    """Write a pickled Python object to a file in chunks"""
    bytes_out = pickle.dumps(data, protocol=4)
    with open(file_path, 'wb') as f_out:
        for idx in range(0, len(bytes_out), MAX_PICKLE_BYTES):
            f_out.write(bytes_out[idx:idx + MAX_PICKLE_BYTES])


def unpickle_big_data(file_path):
    """Return a Python object from a file containing pickled data in chunks"""
    try:
        with open(file_path, 'rb') as f:
            return pickle.load(f)
    except Exception:  # noqa, pylint: disable=broad-except
        bytes_in = bytearray(0)
        input_size = os.path.getsize(file_path)
        with open(file_path, 'rb') as f_in:
            for _ in range(0, input_size, MAX_PICKLE_BYTES):
                bytes_in += f_in.read(MAX_PICKLE_BYTES)
        return pickle.loads(bytes_in)


def get_age_of_file(filename, unit='days'):
    """Return relative age of a file as a datetime.timedelta"""
    age = (datetime.today() - datetime.fromtimestamp(getmtime(filename)))
    return getattr(age, unit)


def get_files_in_directory(directory):
    """Return all files in a directory, non-recursive"""
    return [
        f for f in os.listdir(directory) if
        isfile(join_path(directory, f))
    ]


def delete_old_disk_caches():
    cache_metadata = load_cache_metadata_json()
    new_cache_metadata = deepcopy(cache_metadata)
    cache_changed = False
    for function_name, function_caches in cache_metadata.items():
        if function_name == _TOTAL_NUMCACHE_KEY:
            continue
        to_keep = []
        for function_cache in function_caches:
            max_age_days = int(function_cache['max_age_days'])
            file_name = join_path(DISK_CACHE_DIR, function_cache['file_name'])
            if not file_exists(file_name):
                cache_changed = True
                continue
            if not get_age_of_file(file_name) > max_age_days != UNLIMITED_CACHE_AGE:
                to_keep.append(function_cache)
                continue
            logger.info('Removing stale cache file %s, > %d days', file_name, max_age_days)
            cache_changed = True
            os.remove(file_name)
        if to_keep:
            new_cache_metadata[function_name] = to_keep
    if cache_changed:
        write_cache_file(new_cache_metadata)


def get_disk_cache_for_function(function_name):
    cache_metadata = load_cache_metadata_json()
    return cache_metadata.get(function_name, None)


def get_disk_cache_size_for_function(function_name):
    """Return the current number of entries in the cache for a function by name"""
    function_cache = get_disk_cache_for_function(function_name)
    return None if function_cache is None else len(function_cache)


def delete_disk_caches_for_function(function_name):
    logger.debug('Removing cache entries for %s', function_name)
    n_deleted = 0
    cache_metadata = load_cache_metadata_json()
    if function_name not in cache_metadata:
        return

    functions_to_delete_cache_for = cache_metadata.pop(function_name)
    for function_cache in functions_to_delete_cache_for:
        file_name = join_path(DISK_CACHE_DIR, function_cache['file_name'])
        os.remove(file_name)
        n_deleted += 1
    logger.debug('Removed %s cache entries for %s', n_deleted, function_name)
    write_cache_file(cache_metadata)


def cache_exists(cache_metadata, function_name, *args, **kwargs):
    if function_name not in cache_metadata:
        return False, None
    new_caches_for_function = []
    cache_changed = False
    for function_cache in cache_metadata[function_name]:
        if function_cache['args'] == str(args) and (
                function_cache['kwargs'] == str(kwargs)):
            max_age_days = int(function_cache['max_age_days'])
            file_name = join_path(DISK_CACHE_DIR, function_cache['file_name'])
            if file_exists(file_name):
                if get_age_of_file(file_name) > max_age_days != UNLIMITED_CACHE_AGE:
                    os.remove(file_name)
                    cache_changed = True
                else:
                    function_value = unpickle_big_data(file_name)
                    return True, function_value
            else:
                cache_changed = True
        else:
            new_caches_for_function.append(function_cache)
    if cache_changed:
        if new_caches_for_function:
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
    if function_name == _TOTAL_NUMCACHE_KEY:
        raise Exception(
            'Cant cache function named %s' % _TOTAL_NUMCACHE_KEY)
    function_caches = cache_metadata.get(function_name, [])
    new_file_name = str(int(cache_metadata[_TOTAL_NUMCACHE_KEY]) + 1) + '.pkl'
    new_cache = {
        'args': str(args),
        'kwargs': str(kwargs),
        'file_name': new_file_name,
        'max_age_days': n_days_to_cache
    }
    pickle_big_data(function_value, join_path(DISK_CACHE_DIR, new_file_name))
    function_caches.append(new_cache)
    cache_metadata[function_name] = function_caches
    cache_metadata[_TOTAL_NUMCACHE_KEY] = int(cache_metadata[_TOTAL_NUMCACHE_KEY]) + 1
    write_cache_file(cache_metadata)


def cache_to_disk(n_days_to_cache=DEFAULT_CACHE_AGE):
    """Cache to disk"""
    if n_days_to_cache == UNLIMITED_CACHE_AGE:
        warnings.warn('Using an unlimited age cache is not recommended', stacklevel=3)
    if isinstance(n_days_to_cache, int):
        if n_days_to_cache < 0:
            n_days_to_cache = 0
    elif n_days_to_cache is not None:
        raise TypeError('Expected n_days_to_cache to be an integer or None')

    def decorating_function(original_function):
        wrapper = _cache_to_disk_wrapper(original_function, n_days_to_cache, _CacheInfo)
        return wrapper

    return decorating_function


def _cache_to_disk_wrapper(original_func, n_days_to_cache, _CacheInfo):  # noqa, pylint: disable=invalid-name
    hits = misses = nocache = 0

    def wrapper(*args, **kwargs):
        nonlocal hits, misses, nocache
        cache_metadata = load_cache_metadata_json()
        already_cached, function_value = cache_exists(
            cache_metadata, original_func.__name__, *args, **kwargs)
        if already_cached:
            logger.debug('Cache HIT on %s (hits=%s, misses=%s, nocache=%s)',
                         original_func.__name__, hits, misses, nocache)
            hits += 1
            return function_value

        logger.debug('Cache MISS on %s (hits=%s, misses=%s, nocache=%s)',
                     original_func.__name__, hits, misses, nocache)
        logger.debug(' -- MISS ARGS:    (%s)', ','.join(
            [str(arg) for arg in args]))
        logger.debug(' -- MISS KWARGS:  (%s)', ','.join(
            ['{}={}'.format(str(k), str(v)) for k, v in kwargs.items()]))
        misses += 1

        try:
            function_value = original_func(*args, **kwargs)
        except NoCacheCondition as err:
            nocache += 1
            logger.debug('%s() threw NoCacheCondition exception; no new cache entry', original_func.__name__)
            function_value = err.function_value
        else:
            logger.debug('%s() returned, adding cache entry', original_func.__name__)
            cache_function_value(
                function_value,
                n_days_to_cache,
                cache_metadata,
                original_func.__name__,
                *args,
                **kwargs)
        return function_value

    def cache_info():
        """Report runtime cache statistics"""
        return _CacheInfo(hits, misses, nocache)

    def cache_clear():
        """Clear the cache permanently from disk for this function"""
        logger.info('Cache clear requested for %s(); %s items in cache ...', original_func.__name__, )
        delete_disk_caches_for_function(original_func.__name__)

    def cache_size():
        """Return the number of cached entries for this function"""
        return get_disk_cache_size_for_function(original_func.__name__)

    def cache_get_raw():
        """Return the raw cache object for this function as a list of dicts"""
        warnings.warn('This is an internal interface and should not be used lightly', stacklevel=3)
        return get_disk_cache_for_function(original_func.__name__)

    wrapper.cache_info = cache_info
    wrapper.cache_clear = cache_clear
    wrapper.cache_size = cache_size
    wrapper.cache_get_raw = cache_get_raw
    return wrapper


ensure_dir(DISK_CACHE_DIR)
delete_old_disk_caches()
