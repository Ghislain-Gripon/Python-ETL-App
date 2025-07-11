import logging, functools

def debug(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={repr(v)}" for k, v in sorted(kwargs.items())]
        signature = ", ".join(args_repr + kwargs_repr)
        logging.debug(f"Calling {func.__name__}({signature})")
        value = func(*args, **kwargs)
        logging.debug(f"{func.__name__} returned {repr(value)}")
        return value

    return wrapper