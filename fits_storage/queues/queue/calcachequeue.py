"""CalCacheQueue housekeeping class. Note that this is not the ORM class,
which is now called ...QueueEntry as it represents an entry on the queue as
opposed to the queue itself."""

from .queue import Queue
from ..orm.calcachequeueentry import CalCacheQueueEntry

from fits_storage.core.orm.header import Header
from fits_storage.cal.calibration import get_cal_object
from fits_storage.cal.orm.calcache import CalCache
from fits_storage.cal.associate_calibrations import associate_cals

from sqlalchemy.exc import IntegrityError


class CalCacheQueue(Queue):

    def __init__(self, session, logger=None):
        super().__init__(session, ormclass=CalCacheQueueEntry, logger=logger)

    def add(self, hid, filename, commit=True):
        """
        Add and entry to the CalCacheQueue. This instantiates a
        CalCacheQueueEntry instance, adds it to the database. If commit=True
        (the default) it commits the session.

        Returns
        -------
        False on error
        True on success
        """
        cqe = CalCacheQueueEntry(hid, filename)

        self.session.add(cqe)

        if commit:
            try:
                self.session.commit()
                return True
            except IntegrityError:
                self.logger.debug(f"Integrity error adding file {filename} to "
                                  "CalCache Queue. Most likely, file is already"
                                  " on queue. Silently rolling back.")
                self.session.rollback()
                return False
        else:
            return True

    def cache_associations(self, obs_hid):
        """
        Do the calibration association and insert the associations into the
        calcache table. Remove any old associations that this replaces

        Parameters
        ----------
        obs_hid : int
            ID of Header to associate and cache calibrations for.
        """

        # Get the Header object
        header = self.session.get(Header, obs_hid)

        if None in [header.instrument, header.ut_datetime]:
            return

        if header.qa_state == 'Fail':
            return

        # Get a cal object for it
        cal = get_cal_object(self.session, None, header=header)

        # Loop through the applicable calibration types
        for caltype in cal.applicable:
            # Blow away old associations of this caltype
            self.session.query(CalCache)\
                .filter(CalCache.obs_hid == header.id)\
                .filter(CalCache.caltype == caltype)\
                .delete(synchronize_session=False)
            self.session.commit()

            # Get the associations for this caltype
            cal_headers = associate_cals(self.session, [header],
                                         caltype=caltype)
            for rank, cal_header in enumerate(cal_headers):
                if caltype in ('bpm', 'processed_bpm'):
                    # we want BPMs to appear at the top of the associated cal
                    # tab search results
                    rank = -1
                cc = CalCache(obs_hid, cal_header.id, caltype, rank)
                self.session.add(cc)
            self.session.commit()
