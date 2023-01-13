import re

import datetime
from sqlalchemy import desc, func
from sqlalchemy import Column, UniqueConstraint
from sqlalchemy import Integer, Boolean, Text, DateTime

from gemini_obs_db.db import Base

_standard_filename_re = re.compile('[NS](\d{8}).*')
_igrins_filename_re = re.compile('SDC[HK]_(\d{8})_.*')
_skycam_filename_re = re.compile('img_(\d{8})_.*')
_obslog_filename_re = re.compile('(\d{8})_.*_obslog.txt')

_regexes = [_standard_filename_re,
            _igrins_filename_re,
            _skycam_filename_re,
            _obslog_filename_re]

# lower down regexes in this list get priority over earlier
# destinations not in this list get the lowest priority
_destination_priorities = [
    re.compile('.*archive.*'),
]

def _sortkey(filename, destination):
    """
    Generate a sort key for the given filename.

    For recognized filename patterns, this will extract a datestring.  For
    other filenames, it will prepend a 0000 so that they will sort sensibly
    amongst their set but will come after the files we know about.

    Additionally, a priority number is generated for any special export
    destinations.  These destinations take priority over any lower ranked
    destinations.  This way, all archive exports will be run before anything
    else.  Currently, archive is the only prioritized destination.
    """
    if filename is None:
        return None

    # default
    sortkey = '0000%s' % filename  # push to back of the sort vs any reasonable year value

    for rex in _regexes:
        m = rex.match(filename)
        if m:
            sortkey = m.group(1)

    priority = 0
    for idx, rex in enumerate(_destination_priorities):
        if rex.match(destination):
            priority = 1+idx

    sortkey = f'%03d%s' % (priority, sortkey)
    return sortkey


# ------------------------------------------------------------------------------
class ExportQueue(Base):
    """
    This is the ORM object for the ExportQueue table.

    """
    __tablename__ = 'exportqueue'
    __table_args__ = (
        UniqueConstraint('filename', 'inprogress', 'failed', 'destination'),
    )

    id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=False, unique=False, index=True)
    path = Column(Text)
    destination = Column(Text, nullable=False, unique=False, index=True)
    inprogress = Column(Boolean, index=True)
    failed = Column(Boolean)
    sortkey = Column(Text, index=True)
    added = Column(DateTime)
    lastfailed = Column(DateTime)
    after = Column(DateTime)

    error_name = 'EXPORT'

    def __init__(self, filename, path, destination):
        """
        Create an :class:`~ExportQueue` record

        Parameters
        ----------
        filename : str
            Name of the file to export
        path : str
            Path of the file within the `storage_root`
        destination : str
            URL of the server to export to
        """
        self.filename = filename
        self.sortkey = _sortkey(filename, destination)
        self.path = path
        self.destination = destination
        self.added = datetime.datetime.now()
        self.after = datetime.datetime.min
        self.inprogress = False
        self.failed = False

    @staticmethod
    def find_not_in_progress(session):
        """
        Returns a query that will find the elements in the queue that are not 
        in progress, and that have no duplicates, meaning that there are not two 
        entries where one of them is being processed (it's ok if there's a failed 
        one...)

        Parameters
        ----------
        session : :class:`sqlalchemy.orm.session.Session`
            SQL Alchemy session to query against
        """
        # The query that we're performing here is equivalent to
        #
        # WITH inprogress_filenames AS (
        #   SELECT filename FROM exportqueue
        #                   WHERE failed = false AND inprogress = True
        # )
        # SELECT id FROM exportqueue
        #          WHERE inprogress = false AND failed = false and after <= NOW()
        #          AND filename not in inprogress_filenames
        #          ORDER BY after, sortkey DESC, filename DESC

        inprogress_filenames = (
            session.query(ExportQueue.filename)
                .filter(ExportQueue.failed == False)
                .filter(ExportQueue.inprogress == True)
        )

        return (
            session.query(ExportQueue)
                .filter(ExportQueue.inprogress == False)
                .filter(ExportQueue.failed == False)
                .filter(ExportQueue.after <= datetime.datetime.now())
                .filter(~ExportQueue.filename.in_(inprogress_filenames))
                .order_by(ExportQueue.after, desc(ExportQueue.sortkey), desc(ExportQueue.filename))
        )

# TODO seems to be out of use, removing for now
#     @staticmethod
#     def rebuild(session, element):
#         session.query(ExportQueue)\
#             .filter(ExportQueue.inprogress == False)\
#             .filter(ExportQueue.filename == element.filename)\
#             .delete()

    def __repr__(self):
        """
        Make a string representation of the queue item.

        Returns
        -------
        str : String representation of this record
        """
        return "<ExportQueue('%s', '%s')>" % (self.id, self.filename)


if __name__ == "__main__":
    print(_sortkey("N20221201S0001.fits", "https://archive.gemini.edu"))
    print(_sortkey("random.fits", "https://archive.gemini.edu"))
    print(_sortkey("N20221201S0001.fits", "https://hbffitstape-lp2.hi.gemini.edu"))
    print(_sortkey("random.fits", "https://hbffitstape-lp2.hi.gemini.edu"))
