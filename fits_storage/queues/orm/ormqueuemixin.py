"""
This module contains a class containing utility functions that is mixedin
to the queue ORM classes.
"""

from fits_storage.utils.gemini_metadata_utils import sortkey_regex_dict

class OrmQueueMixin():
    def sortkey_from_filename(self, filename=None):
        """
        Return a key to be used in the database sorting. Used for queues,
        where sorting directly by filename works for facility standard
        filenames but not when we have non-standard filenames in the queue.

        We extract a date (YYYYMMDD) string and a serial number string from
        the filename and form a sortkey based on those.

        We also exert some prioritization by prepending a 'z' to high
        priority files (eg regular science filenames, and other letters to
        lower priority files such as site monitoring data. The sort is done
        in descending order, so 'z' is high priortiy and 'a' is low priority.

        The regexes for this are in a dict imported from gemini_metadata_utils.

        Filenames that do not match the regex are given sortkeys that are
        simply 'aaaa' followed by the filename. This effectively gives them a
        lower priority than any filenames that do match.

        sortkey_regex_dict: These regular expessions are used by the queues
        to determine how to sort ( ie prioritize) files when despooling the
        queues. The regexes should provide two named groups - date (YYYYMMDD)
        and optional num (serial number). The regexes are keys in a dict,
        where the value is a higher level priority. ie files matching regexes
        with value z are considered highest priority, and those matching
        regexes with value x are next in priority.
        """

        if filename is None:
            filename = self.filename

        sortkey = 'aaaa' + filename

        # Note, we can't do a 'for a, b in blah' unpack here as the dictionary
        # key is a compiled regular expression and isn't itterable.
        for cre in sortkey_regex_dict.keys():
            m = cre.match(filename)
            if m:
                sortkey = sortkey_regex_dict[cre] + \
                          m.group('date') + m.group('num')

        return sortkey
    @property
    def failed(self):
        """
        SQLAlchemy doesn't get (v1.4 - there's some discussion of adding this
        to 2.0 but it's not there yet) support the SQL 'NULLS NOT DISTINCT'
        clause in constraints. This means we can't use None (or NULL) as a
        value for failed and still use uniqueness constraints to prevent
        adding duplicate queue entries. So we're using datetime.datetime.max
        as a pseudo null value for entries that have not failed yet.
        """

        if self.failed == datetime.datetime.max:
            return None
        else:
            return self.failed
