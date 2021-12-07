# cache_to_disk
Local disk caching decorator for python functions with auto-invalidation.

This is intended to cache functions that both take a long time to run, and have return values that take up too much memory to cache in-memory with redis. The results of the function are pickled and saved to a file, and then unpickled and returned the next time the function is called. The caching is argument specific, so if the function is called with different arguments, the function will be run again. The caching decorator accepts an integer representing the number of days to cache the function for. After this many days, the file for that function will be deleted the next time this module is imported.

# Installation
```bash
pip install cache_to_disk
```

# Functions:
cache_to_disk(n_days_to_cache)
delete_disk_caches_for_function(function_name)
delete_old_disk_caches()


# Examples:
**cache_to_disk**
```python
"""
This example caches the function "my_function" for 3 days.
"""

from cache_to_disk import cache_to_disk

@cache_to_disk(3)
def my_function():
    to_return = []
    for i in range(10000):
        for j in range(i):
            to_return.append(i * j ** .23)
    return to_return
```
**delete_disk_caches_for_function**

```python
"""
This example invalidates all of the caches for the function "my_function". The function will be invalidated automatically, but this should be used when the function definition has been changed and you want it to re-run.
"""

from cache_to_disk import delete_disk_caches_for_function
delete_disk_caches_for_function('my_function')
```

**runtime_accounting**
```python
"""
This example shows how to check the run-time cache accounting, which shows hits, misses and nocache events
"""

from cache_to_disk import cache_to_disk

@cache_to_disk(3)
def query_registrar(host, port, query):
    socket = tcp_connect(host, port)
    socket.send(query)
    response = b''
    while True:
        buf = read_wrapper(socket)
        if buf is None:
            break
        response += buf
    return response    

query_registrar('whois.verisign-grs.com', 43, b'test.com')
query_registrar('whois.verisign-grs.com', 43, b'test.com')
query_registrar('whois.verisign-grs.com', 43, b'test.com')
query_registrar('whois.verisign-grs.com', 43, b'test.com')
print(query_registrar.cache_info())
```

**nocache**
```python
"""
This example shows how to inhibit caching depending on certain conditions, such as a network failure while also returning a value
"""
from cache_to_disk import cache_to_disk, NoCacheCondition
from random import randint
@cache_to_disk(3)
def query_registrar(host, port, query):
    socket = tcp_connect(host, port)
    socket.send(query)
    response = b''
    while True:
        try:
            if randint(0, 5) > 3:
                # Simulate a spurious failure like SIGPIPE/EPIPE
                raise socket.error
            buf = read_wrapper(socket)
            
            if buf is None:
                break
            response += buf
        except socket.error:
            # To the user, functionalliy requivalent to `return response` except
            # the result is not cached, so it can be retried immediately or later
            raise NoCacheCondition(function_value=response)
    return response

query_registrar('whois.verisign-grs.com', 43, b'test.com')
query_registrar('whois.verisign-grs.com', 43, b'test.com')
query_registrar('whois.verisign-grs.com', 43, b'test.com')
query_registrar('whois.verisign-grs.com', 43, b'test.com')
print(query_registrar.cache_info())
```
