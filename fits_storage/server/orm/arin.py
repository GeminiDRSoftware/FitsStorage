from sqlalchemy import Column, Integer, BigInteger, Text, Date
import ipaddress

from fits_storage.core.orm import Base

from fits_storage.config import get_config

# This is a bit ugly, but stops this erroring when imported on sqlite configs
if get_config().using_sqlite:
    from sqlalchemy.dialects.sqlite import CHAR as CIDR
    from sqlalchemy.dialects.sqlite import CHAR as INET
else:
    from sqlalchemy.dialects.postgresql import INET, CIDR


class ArinAsn(Base):
    """
    ASNs from ARIN
    """

    __tablename__ = 'arinasns'

    id = Column(Integer, primary_key=True)
    reg = Column(Text)
    cc = Column(Text)
    asnstart = Column(BigInteger)
    asnend = Column(BigInteger)
    date = Column(Date)
    status = Column(Text)
    opaqueid = Column(Text)

    def __init__(self, things):
        # Build from pre-split line from arin file
        try:
            self.reg = things[0]
            self.cc = things[1]
            self.asnstart = int(things[3])
            self.asnend = self.asnstart + int(things[4]) - 1
            date = things[5]
            date = None if date in ('00000000', '') else date
            self.date = date
            self.status = things[6].strip()
            if len(things) > 7:
                self.opaqueid = things[7]
        except IndexError:
            print(f'Index Error on {things}')
            raise

class ArinNetwork(Base):
    """
    Networks from ARIN
    """

    __tablename__ = 'arinnetworks'

    id = Column(Integer, primary_key=True)
    reg = Column(Text)
    cc = Column(Text)
    ipstart = Column(INET)
    ipend = Column(INET)
    date = Column(Date)
    status = Column(Text)
    opaqueid = Column(Text)

    def __init__(self, things):
        # Build from pre-split line from arin file
        try:
            self.reg = things[0]
            self.cc = things[1]
            self.ipstart = things[3]
            ipend = ipaddress.ip_address(self.ipstart)
            ipend += int(things[4])
            self.ipend = str(ipend)
            date = things[5]
            date = None if date in ('00000000', '') else date
            self.date = date
            self.status = things[6]
            if len(things) > 7:
                self.opaqueid = things[7]
        except IndexError:
            print(f'Index Error on {things}')
            raise

class ArinPrefix(Base):
    """
    IP Prefixes derived from ARIN data
    """
    __tablename__ = 'arinprefixes'

    id = Column(Integer, primary_key=True)
    prefix = Column(CIDR)
    arinnetwork_id = Column(Integer)
