from functools import wraps
import contextlib
import os
import fcntl
import logging

log = logging.getLogger(__name__)

def _flock_acquire(lock_path):
    """Acquire lock file at given path.

    Lock is exclusive, errors return immediately instead of waiting."""
    open_mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
    fd = os.open(lock_path, open_mode)
    try:
        # LOCK_EX = exclusive
        fcntl.flock(fd, fcntl.LOCK_EX)
    except Exception as e:
        os.close(fd)
        log.warning('failed to get lock at ' + lock_path + ": " + str(e))
        raise

    return fd


def _flock_release(fd):
    """Release lock file at given path."""
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@contextlib.contextmanager
def _flock_contextmanager(lock_path):
    """Creates context with exclusive file lock at given path."""
    fd = _flock_acquire(lock_path)
    try:
        yield
    finally:
        _flock_release(fd)


def flock(func):
    """Function decorator that makes use of exclusive file lock to ensure
    only one function instance is running at a time.

    All function runners must be present on the same container for this to work."""
    LOCK_FILE_BASE = '/tmp'
    file_name = f'_flock_{func.__name__}.lock'
    lock_path = os.path.join(LOCK_FILE_BASE, file_name)

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            with _flock_contextmanager(lock_path):
                return func(*args, **kwargs)
        except Exception as e:
            log.warning('function already running: %s, %s', func.__name__, str(e))
            return
    return wrapper
