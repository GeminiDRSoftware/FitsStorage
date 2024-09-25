import datetime

from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, DateTime
from sqlalchemy.orm import relationship

from fits_storage.logger import DummyLogger

from fits_storage.core.orm import Base
from fits_storage.server.orm.usagelog import UsageLog
from fits_storage.server.orm.ipprefix import IPPrefix

from fits_storage.db import sessionfactory
from fits_storage.server.prefix_helpers import get_ipprefix
from fits_storage.gemini_metadata_utils.telescope_instruments import \
    obs_types, obs_classes


class UsageLogAnalysis(Base):
    """
    This table stores results on usagelog entry analysis. We keep this in
    a separate table from usagelog to make updates to this tables schema
    easier. It has a [0|1]:1  relationship with usagelog - usagelog
    rows that have not been analysed will not have entries in this table.

    The score numbers stored here are intended to be specifically from the
    individual usagelog_entry.
    """
    __tablename__ = 'usagelog_analysis'
    id = Column(Integer, primary_key=True)
    usagelog_id = Column(Integer, ForeignKey('usagelog.id'), nullable=False,
                         index=True)
    usagelog = relationship(UsageLog, order_by=id)

    utc_analysed = Column(DateTime(timezone=False))

    prefix_id = Column(Integer, ForeignKey('ipprefix.id'), index=True)
    prefix = relationship(IPPrefix, order_by=id)
    asn = Column(Integer, index=True)

    uri_score = Column(Integer)
    agent_score = Column(Integer)
    referer_score = Column(Integer)
    total_score = Column(Integer)

    def __init__(self, usagelog_id):
        """
        Initialize an usagelog_analysis entry. Must provide usagelog_id
        """

        self.usagelog_id = usagelog_id

        self.url_score = 0
        self.agent_score = 0
        self.referer_score = 0
        self.total_score = 0

    def analyse(self, api=None, logger=DummyLogger()):
        """
        Analyse the usagelog entry, update values.
        """
        logger.debug("Analysing usagelog entry %d", self.usagelog_id)
        self.utc_analysed = datetime.datetime.utcnow()

        self.uri_score = score_uri(self.usagelog.uri)
        self.agent_score = score_agent(self.usagelog.user_agent)
        self.referer_score = score_referrer(self.usagelog.referer)

        # We don't need a column to store this, as it's a direct lookup
        # from usagelog. Invalid URLs incur a 5 point penalty
        status_score = 5 if self.usagelog.status == 404 else 0

        self.total_score = self.uri_score + self.agent_score + \
                           self.referer_score + status_score
        logger.debug("Total score: %d", self.total_score)

        try:
            with sessionfactory() as session:
                ipp = get_ipprefix(session, self.usagelog.ip_address, api=api,
                                   logger=logger)
                if ipp is not None:
                    self.prefix_id = ipp.id
                    self.asn = ipp.asn
        except Exception:
            logger.error("Exception getting prefix.", exc_info=True)


def _score_obs(url_words, obsthings):
    """
    Scoring code that is used for both obsclasses and obstypes
    """
    score = 0

    # We want to detect FLAT/ARC as well as FLAT/FLAT.
    # But FLAT/FLAT is considered doubly bad.
    n_obsthings = 0
    for i in obsthings:
        n = url_words.count(i)
        n_obsthings += n
        # Penalize having the same obsthing multiple times
        if n > 1:
            score += 2 * n

    # Penalize having multiple obsthings. Yes, there's a deliberate double
    # jeopardy going on here.
    if n_obsthings > 1:
        score += n_obsthings

    return score


def score_uri(uri):
    """
    Generate a "badness" score for a URI. 0 is neutral, negative score suggests
    URL may come from a good robot or user. positive score suggests
    bad user.

    Multiple obsclasses or obstypes in the URL very much suggest this is
    blind link following (ie a rampaging robot)
    """
    uri_words = uri.split('/')
    score = 0

    score += _score_obs(uri_words, obs_classes)
    score += _score_obs(uri_words, obs_types)

    return score


def score_referrer(ref):
    """
    Generate a "badness" score for a http referrer. 0 is neutral, positive
    score is bad.

    Not implemented yet. Will likely need to be driven by a lookup, avoid
    hard-coding values here.
    """
    return 0


def score_agent(agent):
    """
        Generate a "badness" score for a http user agent. 0 is neutral, positive
        score is bad.

        Not implemented yet. Will likely need to be driven by a lookup, avoid
        hard-coding values here.
        """
    return 0
