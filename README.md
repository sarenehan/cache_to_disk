# cache_to_disk
Local disk caching decorator for python functions.

This is intended for functions with large return objects that would clog up a local redis memory cache. The results of the function are pickled and saved to a file, and then unpickled and returned the next time the function is called. The caching is argument specific, so if the function is called with different arguments, the function will be run again.

# Functions:
cache_to_disk(n_days_to_cache)
delete_disk_caches_for_function()
delete_old_disk_caches()


# Example:
from cache_to_disk import cache_to_disk

@cache_to_disk(3)
def run_function():
    to_return = []
    for i in range(10000):
        for j in range(i):
            to_return.append(i * j)


