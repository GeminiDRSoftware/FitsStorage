"""
This module contains a class containing utility functions that is mixed-in
to the queue ORM classes.
"""
import datetime
import os
import fcntl

from fits_storage.gemini_metadata_utils import sortkey_regex_dict
from fits_storage.config import get_config

class OrmQueueMixin:
    def sortkey_from_filename(self, filename=None):
        """
        Return a key to be used in the database sorting. Used for queues,
        where sorting directly by filename works for facility standard
        filenames but not when we have non-standard filenames in the queue.

        We extract a date (YYYYMMDD) string and a serial number string from
        the filename and form a sortkey based on those.

        We also exert some prioritization by prepending a 'z' to high
        priority files (e.g. regular science filenames), and other letters to
        lower priority files such as site monitoring data. The sort is done
        in descending order, so 'z' is high priority and 'a' is low priority.

        The regexes for this are in a dict imported from gemini_metadata_utils.

        Filenames that do not match the regex are given sortkeys that are
        simply 'aaaa' followed by the filename. This effectively gives them a
        lower priority than any filenames that do match.

        sortkey_regex_dict: These regular expressions are used by the queues
        to determine how to sort ( ie prioritize) files when despooling the
        queues. The regexes should provide two named groups - date (YYYYMMDD)
        and optional num (serial number). The regexes are keys in a dict,
        where the value is a higher level priority. ie files matching regexes
        with value z are considered the highest priority, and those matching
        regexes with value x are next in priority.
        """

        if filename is None:
            filename = self.filename

        sortkey = 'aaaa' + filename

        # Note, we can't do a 'for a, b in blah' unpack here as the dictionary
        # key is a compiled regular expression and isn't iterable.
        for cre in sortkey_regex_dict.keys():
            m = cre.match(filename)
            if m:
                sortkey = sortkey_regex_dict[cre] + \
                          m.group('date') + m.group('num')

        return sortkey

    # This is the magic value to represent failed = False.
    fail_dt_false = datetime.datetime.max

    @property
    def failed(self):
        """
        SQLAlchemy doesn't yet (v1.4 - there's some discussion of adding this
        to 2.0, but it's not there yet) support the SQL 'NULLS NOT DISTINCT'
        clause in constraints. This means we can't use None (or NULL) as a
        value for failed and still use uniqueness constraints to prevent
        adding duplicate queue entries. So we're using a magic value aka
        fail_dt_false which is actually datetime.datetime.max
        as a pseudo null value for fail_dt for entries that have not failed yet.

        What we really want from the user side is a True / False value with
        the unique constraint only applied to False values. That's not a thing,
        so we wanted to use a datetime where None implies False, but we can't
        do that because of the NULLS NOT DISTINCT issue. This property
        implements that True False interface using the .max workaround.
        """

        if self.fail_dt == self.fail_dt_false:
            return False
        else:
            return True

    @failed.setter
    def failed(self, failed):
        if failed is True:
            self.fail_dt = datetime.datetime.now()
        elif failed is False or failed is None:
            self.fail_dt = self.fail_dt_false
        else:
            self.fail_dt = failed

    @property
    def fullpathfilename(self):
        """
        Calculates the full path filename - ie concatenation of:
        storage_root, self.path, self.filename

        Returns
        -------
        The full path filename of the filename for this queue entry
        """
        fsc = get_config()
        self.path = '' if self.path is None else self.path
        self.storage_root = fsc.storage_root if self.storage_root is None \
            else self.storage_root
        return os.path.join(self.storage_root, self.path, self.filename)


    @property
    def file_exists(self):
        """
        Determines whether the file exists on disk at the path in the
        storage_root.

        Returns
        -------
        Bool to say whether file exists
        """
        fpfn = self.fullpathfilename
        return os.access(fpfn, os.F_OK | os.R_OK) and os.path.isfile(fpfn)

    @property
    def filelastmod(self):
        """
        Reads the lastmod (mtime) timestamp of the file.
        This will be in the local timezone

        Returns
        -------
        datetime.datetime value for the lastmod time of the file
        """
        return datetime.datetime.fromtimestamp(
            os.path.getmtime(self.fullpathfilename))

    @property
    def file_is_locked(self):
        """
        Try to determine if the file is locked. If we don't have access to
        open the file, then we can't tell and will assume it is not locked.

        Returns
        -------
        True if the file is definitely locked
        False otherwise.
        """

        try:
            with open(self.fullpathfilename, "r+") as fd:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except IOError:
                    return True
        except IOError:
            # Probably don't have write permission to the file
            pass

        return False

    def seterror(self, message):
        """
        Convenience function to call when a queue entry has an
        error during processing. Appends to the error message, sets failed
        to be true and inprogress to be false.
        Note - this method does not commit the session, the caller must do that.

        Parameters
        ----------
        message - error message to record

        Returns
        -------
        Nothing
        """
        if self.error is None:
            self.error = message
        else:
            self.error += '\n\n' + message

        self.inprogress = False
        self.failed = True