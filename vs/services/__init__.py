import asyncio
import sys
import traceback


def retry_handler(ex, test=False):
    if not test:
        traceback.print_exc(file=sys.stdout)
        print(repr(ex), 'retry')


def retry(n=5, wait_factor=2, initial_wait=1, test=False):
    def wrapper(fnc):
        async def inner(*args, **kwargs):
            wait, c = initial_wait, 1
            while c <= n:
                try:
                    return await fnc(*args, **kwargs)
                except Exception as e:
                    retry_handler(e, test)
                    await asyncio.sleep(wait)
                    wait *= wait_factor
                c += 1
            raise TimeoutError('Function `{}` exceeded maximum allowed number of retries'.format(fnc.__name__))
        return inner
    return wrapper
