from sqlalchemy import Column, Integer, Text, Boolean

from fits_storage.core.orm import Base


class ProcessingTag(Base):
    """
    This is the ORM object for the ProcessingTag table, which stores metadata
    about processing tags"""

    __tablename__ = 'processingtag'

    # We should consider making tag and/or domain ENUMs as that will probably
    # help with search performance. Tag would be in reduction too, and we would
    # need a user-friendly way to add values.

    id = Column(Integer, primary_key=True)
    tag = Column(Text, nullable=False, unique=True, index=True)
    domain = Column(Text, nullable=False, index=True)
    priority = Column(Integer, nullable=False, index=True)  # Higher is higher
    published = Column(Boolean, index=True)
    description = Column(Text)

    def __init__(self, tag, domain, priority=0, published=False, description=""):
        self.tag = tag
        self.domain = domain
        self.priority = priority
        self.published = published
        self.description = description

    def __repr__(self):
        return (f"Processing tag id: {self.id:3} - tag: {self.tag:20}, "
                f"domain: {self.domain:16}, priority: {self.priority:4}, "
                f"published: {bool(self.published):5}, "
                f"description: {self.description}")
