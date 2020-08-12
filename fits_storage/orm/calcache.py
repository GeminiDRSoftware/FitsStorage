from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, BigInteger, SmallInteger, Enum, DateTime

from . import Base

from ..gemini_metadata_utils import cal_types

CALTYPE_ENUM = Enum(*cal_types, name='caltype')

# ------------------------------------------------------------------------------
class CalCache(Base):
    """
    This is the ORM class for the calibration Association Cache. It's too slow 
    to do all the calibration association in real time in the archive context,
    so we implement this cache table. We refresh recent observations in the cache
    periodically via cron.

    The obs_hid is the header_id of the thing (observation) to be calibrated
    the cal_hid is the header_id of the calibration that applies the rank is the
    rank of the calibration - ie 0 is the best one (generally closest in time),
    1 is next best, etc caltype is the calibration type.

    """
    __tablename__ = 'calcache'

    id = Column(BigInteger, primary_key=True)
    obs_hid = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    cal_hid = Column(Integer, ForeignKey('header.id'), nullable=False, index=True)
    rank = Column(SmallInteger, nullable=False, index=True)
    caltype = Column(CALTYPE_ENUM, index=True)
    
    def __init__(self, obs_hid, cal_hid, caltype, rank):
        """
        Create a calcache record linking data to a relevant calibration file.

        A calcache record allows us to quickly lookup matching calibrations without
        having to redo the very expensive types of queries that normally requires.
        In essense, we do the query up front and save the results in a pure
        data-to-calibration lookup table for the calibration type.

        Parameters
        ----------

        obs_hid : int
            ID of the :class:`~header.Header` record for the data being linked
        cal_hid : int
            ID of the :class:`~header.Header` record for the calibration
        caltype : `CALTYPE_ENUM`
            type of calibration being linked
        rank : int
            rank of this calibrtion (relative to other options, lower is better)
        """
        self.obs_hid = obs_hid
        self.cal_hid = cal_hid
        self.caltype = caltype
        self.rank = rank
