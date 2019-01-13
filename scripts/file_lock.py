"""
This script implements a simple wrapper that utilizes zc lock.

A context manager is also provided, and it can be used as follows:
    >>with file_lock('/path/to/lock/file', verbose=True) as fl:
    >>    print fl
    >>    // Do some other things.

"""
from contextlib import contextmanager
from zc.lockfile import LockError, LockFile


@contextmanager
def file_lock(lock_file, verbose=False):
    """
    Context manager for FileLock.
    :param lock_file: string - full path to file that should contain lock.
    :param verbose: boolean - verbosity, False as default.
    :return: File lock object if lock was successfully acquired, None otherwise.
    """
    fl = FileLock()
    fl.acquire_lock(lock_file=lock_file, verbose=verbose)
    yield fl.lock
    del fl


class FileLock(object):
    """
    Wrapper class for locking a file.

    To use this class, simply inherit it then call the acquire_lock method:
        >> super(self.__class__, self).__init__()
        >> self.acquire_lock(lock_file=LOCK_FILE)
    """

    def __init__(self):
        """
        Initialize the lock.
        """
        self.lock = None

    def __del__(self):
        """
        Release lock on exist.
        """
        if self.lock:
            self.lock.close()

    def acquire_lock(self, lock_file, verbose=False):
        """
        Attempts to acquire lock. This is to ensure that there is only one process
        running this script.
        :param lock_file: string - full path to file that should contain lock.
        :param verbose: boolean - verbosity, False as default.
        :return: File lock object if lock was successfully acquired, None otherwise.
        """
        try:
            self.lock = LockFile(lock_file, content_template='{pid}@{hostname}')
            if verbose:
                print("Lock Acquired!")
            fd = open(lock_file)
            if verbose:
                print("Lock process: {}".format(fd.read()))
            return self.lock
        except LockError:
            if verbose:
                print("Lock has already been acquired. Exiting")
            return None

    def release_lock(self):
        """
        Releases a previously acquired lock.
        """
        if self.lock:
            self.lock.close()
