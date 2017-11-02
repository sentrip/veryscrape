import asyncio
from functools import wraps

LOG_LEVEL = 'DEBUG'


def synchronous(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        l = asyncio.get_event_loop()
        return l.run_until_complete(f(*args, **kwargs))
    return wrapper
