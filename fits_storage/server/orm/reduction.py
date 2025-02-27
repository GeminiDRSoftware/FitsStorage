from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text

from fits_storage.core.orm import Base

from fits_storage.core.orm.header import PROCMODE_ENUM
from fits_storage.gemini_metadata_utils import gemini_processing_modes

from fits_storage.logger_dummy import DummyLogger


class Reduction(Base):
    """
    This is the ORM object for the Reduction table, which stores metadata about
    the reduction status of data.
    """

    __tablename__ = 'reduction'

    id = Column(Integer, primary_key=True)
    header_id = Column(Integer, ForeignKey('header.id'), nullable=False,
                         index=True)

    processing_intent = Column(PROCMODE_ENUM, index=True)
    software_mode = Column(PROCMODE_ENUM)
    software_used = Column(Text)
    software_version = Column(Text)
    processing_initiated_by = Column(Text)
    processing_reviewed_by = Column(Text)
    processing_review_outcome = Column(PROCMODE_ENUM)
    processing_level = Column(Integer, index=True)
    processing_tag = Column(Text)

    def __init__(self, header, diskfile=None, logger=DummyLogger()):
        self.header_id = header.id

        logger.debug("Getting ad object")
        df = diskfile if diskfile is not None else header.diskfile
        ad = df.get_ad_object

        self.processing_intent = self._get_processing_intent(ad)
        self.software_mode = self._get_software_mode(ad)
        self.software_used = ad.phu.get('PROCSOFT')
        self.software_version = ad.phu.get('PROCSVER')
        self.processing_initiated_by = ad.phu.get('PROCINBY')
        self.processing_reviewed_by = ad.phu.get('PROCRVBY')
        self.processing_review_outcome = self._get_processing_review_outcome(ad)
        self.processing_level = self._get_processing_level(ad)
        self.processing_tag = ad.phu.get('PROCTAG')

    def _get_processing_intent(self, ad):
        pi = ad.phu.get('PROCITNT')
        return pi if pi in gemini_processing_modes else None

    def _get_software_mode(self, ad):
        sm = ad.phu.get('PROCMODE')
        return sm if sm in gemini_processing_modes else None

    def _get_processing_review_outcome(self, ad):
        ro = ad.phu.get('PROCREVW')
        valid = (*gemini_processing_modes, 'FAIL')
        return ro if ro in valid else None

    def _get_processing_level(self, ad):
        # If there's a valid proclevl header, always respect it, If it is
        # not an integer, return None.
        if 'PROCLEVL' in ad.phu.keys():
            try:
                proclevl = int(ad.phu.get('PROCLEVL'))
            except (ValueError, TypeError):
                proclevl = None
            return proclevl

        # Otherwise, Return 0 for raw data
        if 'RAW' in ad.tags:
            return 0

        # Otherwise, return None
        return None
