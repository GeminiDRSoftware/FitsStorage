"""
ORM Definitions for Target Data.

Provides data structures for recording the presence of various targets (Jupiter, etc.) as
potentially in the field of view of an observation.
"""
import datetime
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, Text, DateTime, Boolean, BigInteger, String
from sqlalchemy import desc, func

from sqlalchemy.orm import relation, relationship

from fits_storage.orm.diskfile import DiskFile
from . import Base, pg_db, session_scope
from ..utils.queue import sortkey_for_filename


class Target(Base):
    """
    This is the ORM object for the Target table.
    Each row in this table represents a target that can be
    present in an observation.
    """
    __tablename__ = 'target'

    id = Column(Integer, primary_key=True)
    name = Column(String(32), nullable=False, unique=True, index=True)
    ephemeris_name = Column(String(32), unique=True, index=True)

    def __init__(self, name, ephem_name):
        """
        Create a :class:`~Target` record from the given inputs

        Parameters
        ----------
        name : str
            Human-readable name of the target
        ephem_name : str
            ID of the target in the JPL ephemeris
        """
        self.name = name
        self.ephemeris_name = ephem_name


class TargetPresence(Base):
    """
    This is the ORM object for the presence of a target within the field of view of an observation.

    """
    __tablename__ = 'target_presence'

    def __init__(self, diskfile_id, target_name):
        self.target_name = target_name
        self.diskfile_id = diskfile_id

    id = Column(Integer, primary_key=True)

    diskfile = relation(DiskFile)
    target = relation(Target)

    target_name = Column(String(32), ForeignKey('target.name'), nullable=False, index=True)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)


class TargetsChecked(Base):
    """
    This is the ORM object for recording that an observation was checked against all supported
    targets for overlap.

    Having this table allows TargetPresence to be small, only holding the actual presence information.
    """
    __tablename__ = 'targets_checked'

    def __init__(self, diskfile_id, date_checked):
        self.diskfile_id = diskfile_id
        self.date_checked = date_checked

    id = Column(Integer, primary_key=True)

    diskfile = relation(DiskFile)

    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, unique=True, index=True)
    date_checked = Column(DateTime, index=True)


class TargetQueue(Base):
    """
    This is the ORM object for the queue to calculate target presence
    """
    __tablename__ = 'targetqueue'
    __table_args__ = (
        UniqueConstraint('diskfile_id', 'inprogress', 'failed'),
    )

    id = Column(Integer, primary_key=True)

    diskfile = relation(DiskFile)

    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)
    inprogress = Column(Boolean, index=True)
    failed = Column(Boolean)
    added = Column(DateTime)
    force = Column(Boolean)
    after = Column(DateTime)
    sortkey = Column(Text, index=True)

    error_name = 'TARGET'

    def __init__(self, diskfile):
        """
        Create a :class:`~TargetQueue` record from the given :class:`~DiskFile`

        Parameters
        ----------
        diskfile : :class:`~DiskFile`
            File on disk to find target matches for based on the footprint
        """
        self.diskfile_id = diskfile.id
        self.added = datetime.datetime.now()
        self.inprogress = False
        self.force_md5 = False
        self.force = False
        self.after = self.added
        self.failed = False

        # Sortkey is used to sort the order in which we de-spool the queue.
        self.sortkey = diskfile.lastmod.strftime('%Y%m%d')

    @staticmethod
    def find_not_in_progress(session):
        """
        Returns a query that will find the elements in the queue that are not 
        in progress, and that have no duplicates, meaning that there are not two
        entries where one of them is being processed (it's ok if there's a failed 
        one...)

        Parameters
        ----------
        session : :class:`~sqlalchemy.orm.session.Session`
            Session to query against

        Returns
        -------
        :class:`~sqlalchemy.orm.query.Query` : query object for retrieving records

        """
        # The query that we're performing here is equivalent to
        #
        # WITH inprogress_diskfiles AS (
        #   SELECT diskfile_id FROM targetqueue
        #                   WHERE failed = false AND inprogress = True
        # )
        # SELECT targetqueue.id FROM targetqueue, diskfile
        #          WHERE inprogress = false AND failed = false
        #          AND diskfile_id not in inprogress_diskfiles
        #          AND diskfile.id == diskfile_id
        #          ORDER BY diskfile.lastmod DESC

        inprogress_diskfiles = (session.query(TargetQueue.diskfile_id)
                .filter(TargetQueue.failed == False)
                .filter(TargetQueue.inprogress == True)
                .subquery()
        )

        return (
            session.query(TargetQueue)
                .filter(TargetQueue.inprogress == False)
                .filter(TargetQueue.failed == False)
                .filter(TargetQueue.after < datetime.datetime.now())
                .filter(~TargetQueue.diskfile_id.in_(inprogress_diskfiles))
                .order_by(desc(TargetQueue.sortkey))
        )

# TODO I believe this is no longer in use, removing - delete it if it ends up not needed
    # @staticmethod
    # def rebuild(session, element):
    #     session.query(TargetQueue)\
    #         .filter(TargetQueue.inprogress == False)\
    #         .filter(TargetQueue.header_id == element.header_id)\
    #         .delete()

    def __repr__(self):
        """
        Build a string representation of this queue record

        Returns
        -------
        str : String representation of the queue entry
        """
        return "<TargetQueue('{}', '{}')>".format((self.id, self.diskfile_id))


def init_target_tables(session, pg_db):
    """
    Initialize the target tables and build the readable name - JPL id mapping

    Parameters
    ----------
    session : :class:`sqlalchemy.orm.session.Session`
        SQL Alchemy session
    pg_db : :class:`sqlalchemy.engine.interfaces.Connectable`
        Database connection information
    """
    Target.metadata.create_all(bind=pg_db)
    TargetPresence.metadata.create_all(bind=pg_db)
    TargetsChecked.metadata.create_all(bind=pg_db)
    TargetQueue.metadata.create_all(bind=pg_db)

    targets = [
        ('Mercury', 'mercury'),
        ('Venus', 'venus'),
        ('Moon', 'moon'),
        ('Mars', 'mars'),
        ('Jupiter', 'jupiter'),
        ('Saturn', 'saturn'),
        ('Uranus', 'uranus'),
        ('Neptune', 'neptune'),
        ('Pluto', 'pluto')
    ]

    for (a, b) in targets:
        if session.query(Target).filter(Target.ephemeris_name == b).one_or_none() is None:
            session.add(Target(a,b))


if __name__ == "__main__":
    with session_scope() as session:
        init_target_tables(session, pg_db)
