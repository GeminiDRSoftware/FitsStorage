"""
This implements a simple "pidfile" context manager which we use to limit how
many instances of a task are running. Each instance gets passed a
"name" from the calling process, which is used to generate a pid file name.

If the pid file exists, we check if it is state (ie the pid in it reflects a
process which doesn't exist or is not ours) and raise an exception if so.

Otherwise, we write our current pid into the pid file and return.

For convenience in code that calls this, you can pass dummy=True to
effectively disable all the functionality when pid files are not required.

Note: We don't do any file locking when accessing the pid files here. This
leaves us open to a race condition if multiple processes check the same pid
file simultaneously. This doesn't get used in rapid-fire situations, so this
is unlikely to be a problem.
"""
import sys
import os
from fits_storage.config import get_config

fsc = get_config()


class PidFileError(Exception):
    pass


class PidFile(object):
    def __init__(self, logger, name=None, dummy=False):
        filename = sys.argv[0]
        if name is not None:
            filename += "-%s" % name
        self.filename = os.path.join(fsc.lockfile_dir, filename + '.lock')
        self.logger = logger
        self.dummy = dummy

    def __enter__(self):
        """
        If there's no PID file: create a new one and return the context manager.

        If there is PID file:
            If we can read it and the pid doesn't exist or is not ours then
            remove it, create a new one and return the context manager

            If we can read it and the pid is one of ours, bail out

        """

        if self.dummy:
            return self

        if os.path.exists(self.filename):
            self.logger.info("Lockfile {} exists; testing for viability".
                             format(self.filename))
            try:
                try:
                    with open(self.filename, 'r') as lfd:
                        oldpid = int(lfd.read())
                except OSError:
                    raise PidFileError("Could not read PID from lockfile {}"
                                       .format(self.filename))
                # Test if we have any control over the process with that PID
                os.kill(oldpid, 0)
                raise PidFileError(
                    "Lockfile {} refers to PID {} which appears to be valid"
                    .format(self.filename, oldpid))
            except (TypeError, ValueError):
                raise PidFileError("Cannot recognize the PID in lockfile {}"
                                   .format(self.filename))
            except OSError:
                # This is ok - go on
                self.logger.info("PID {} in lockfile refers to a process which "
                                 "either doesn't exist, or is not ours."
                                 .format(oldpid))

            self.logger.info("Removing the stale PID file")
            os.unlink(self.filename)
        else:
            self.logger.info("Lockfile {} does not exist".
                             format(self.filename))

        self.logger.info("Creating new lockfile {}".format(self.filename))
        with open(self.filename, 'w') as lfd:
            lfd.write('{}\n'.format(os.getpid()))

        return self

    def __exit__(self, exc_type, exc_value, tb):
        if not self.dummy:
            self.logger.info("Removing the PID file")
            os.unlink(self.filename)

        if exc_type is not None:
            raise
