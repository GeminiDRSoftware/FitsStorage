from sqlalchemy import Column, Integer, Text, Date

from fits_storage.core.orm import Base

class Semester(Base):
    """
    It's useful in generating statistics to have a table defining semesters.
    These are given as Dates rather than DateTimes, with the start of a
    semester being the same as the end of the previous one. This should
    facilitate correct handling of Chile nights at the boundary.

    """

    __tablename__ = 'semester'

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False, index=True)
    start = Column(Date)
    end = Column(Date)

    def __init__(self, name, start, end):
        self.name = name
        self.start = start
        self.end = end
