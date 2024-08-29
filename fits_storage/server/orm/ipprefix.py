from sqlalchemy import Column
from sqlalchemy import Integer, Text, DateTime, Boolean
from sqlalchemy.dialects.postgresql import CIDR

from fits_storage.core.orm import Base
from fits_storage.config import get_config

# This is a bit ugly, but stops this erroring when imported on sqlite configs
if get_config().using_sqlite:
    from sqlalchemy.dialects.sqlite import CHAR as CIDR
else:
    from sqlalchemy.dialects.postgresql import CIDR

class IPPrefix(Base):
    """
    This is an ORM class for storing IP address prefixes that we have looked up
    on a BGP API. We store these locally to avoid repeat API calls and to
    provide fast lookups of known prefixes. We also maintain a "badness"
    score for each prefix, driven by log analysis, and record here whether
    the prefix should be denied or allowed (ie prevented from being denied).
    We use postgres ip address types here to facilitate ip-in-cidr type queries.
    """

    __tablename__ = 'ipprefix'
    id = Column(Integer, primary_key=True)
    prefix = Column(CIDR, nullable=False, unique=True, index=True)
    api_used = Column(Text)
    api_query_utc = Column(DateTime)
    asn = Column(Integer)
    name = Column(Text)
    description = Column(Text)
    parent = Column(CIDR)
    badness = Column(Integer)
    allow = Column(Boolean)
    deny = Column(Boolean)

    def __init__(self):
        """
        We set the prefix data members directly from the caller when we
        instantiate, as we'd have to pass them all anyway. We set default
        values on the housekeeping data here.
        """
        self.deny = False
        self.allow = False
        self.badness = 0

    def __repr__(self):
        return f"{self.prefix} : Name: {self.name}, Desc: {self.description}"
