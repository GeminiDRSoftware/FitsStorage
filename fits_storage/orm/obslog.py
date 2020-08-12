"""
The Oblog class attempts to determine the observation date and program id from 
the obslog filename. Nominally, such a filename looks like,

    <yyyymmdd>_<progid>_obslog.txt>.

However, certain obslogs (only Phoenix instrument obslogs as far as is known)
have names like, 

    <yyyymmmdd>_obslog.txt,

where mmm is string of the form 'Apr' or 'May', etc. .

A re.match() is attempted on the first regex pattern, which matches the first
filename kind. If that match returns None, a second regex pattern match is 
attempted with a regex pattern matching the second possible form of the filename.
The MONMAP provides the mapping from month name to month number, which must be
used for building datetime.datetime objects.

"""
import re
import datetime
import dateutil.parser

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, Date
from sqlalchemy.orm import relation

from . import Base
from .diskfile import DiskFile
# ------------------------------------------------------------------------------
MONMAP = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
          'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}
OBSLOG_CRE = re.compile(r'^(20\d\d\d\d\d\d)_(.*)_obslog.txt')
OBSLOG_RE2 = re.compile(r'(^\d\d\d\d)([a-zA-Z][a-zA-Z][a-zA-Z])(\d\d)_obslog.txt')


class Obslog(Base):
    """
    This is the ORM class for the obslog table
    """
    __tablename__ = 'obslog'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer,ForeignKey('diskfile.id'), nullable=False, index=True)
    diskfile = relation(DiskFile, order_by=id)
    program_id = Column(Text, index=True)
    date = Column(Date, index=True)

    def __init__(self, diskfile):
        """
        Create an :class:`~Obslog` record based on the provided :class:`~DiskFile`

        Parameters
        ----------
        diskfile : :class:`~DiskFile`
            DiskFile to build record from
        """
        self.diskfile_id = diskfile.id
        filename = diskfile.filename
        if filename:
            match = OBSLOG_CRE.match(filename)
            if match:
                datestr = match.group(1)
                self.date = dateutil.parser.parse(datestr).date()
                self.program_id = match.group(2)
            else:
                match = OBSLOG_RE2.match(filename)
                if match:
                    self.date = self._secondary_match(match)
                    self.program_id = None

    def __repr__(self):
        """
        Get a string representation of this Obslog

        Returns
        -------
        str : String representation of the :class:`~Obslog`
        """
        report = "<Obslog('{}', '{}', '{}')>"
        return report.format(self.id, self.program_id, self.date)

    def _secondary_match(self, match):
        year = int(match.group(1))
        day = int(match.group(3))
        try:
            mon = MONMAP[match.group(2).lower()]
        except KeyError:
            # if we still haven't scrapped a date from the filename,
            # fix it to an quasi-arbitrary date; 1990-01-01.
            return datetime.datetime(1990, 1, 1).date()

        return datetime.datetime(year, mon, day)
