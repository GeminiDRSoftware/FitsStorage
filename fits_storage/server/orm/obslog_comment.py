from sqlalchemy import Column
from sqlalchemy import Integer, Text

from fits_storage.core.orm import Base


class ObslogComment(Base):
    """
    This is the ORM class for storing observation log comments retrieved from
    the ODB. Note, these are not related (in the FitsStorage world) to the
    obslog files we also store.
    """
    __tablename__ = 'obslog_comment'

    id = Column(Integer, primary_key=True)
    data_label = Column(Text, index=True)
    comment = Column(Text)

    # Note - we don't define data_label to be a foreign key into header,
    # because we want to allow the ingest of obslog comments for which the
    # fits file hasn't been ingested (yet). So we define the foreign key
    # in the *relationship* in the header table, but not as a constraint
    # in the obslog_comment table.

    def __init__(self, data_label, comment):
        """
        Create an ObslogComment record for the given data label, and comment
        """
        self.data_label = data_label
        self.comment = comment
