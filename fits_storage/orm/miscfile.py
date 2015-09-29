from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import relation

from . import Base, NoResultFound
from .diskfile import DiskFile

from ..fits_storage_config import using_s3
if using_s3:
    from ..utils.aws_s3 import get_helper, ClientError
    s3 = get_helper()

import json
import os

class FileClash(Exception):
    pass

MISCFILE_PREFIX = 'miscfile_'

def normalize_diskname(filename):
    if not filename.startswith(MISCFILE_PREFIX):
        return MISCFILE_PREFIX + filename

    return filename

def miscfile_meta_path(path):
    return path + '.json'

def is_miscfile(path):
    if os.path.exists(miscfile_meta_path(path)):
        return True
    elif using_s3:
        try:
            md = s3.get_key(path).metadata
            return 'is_misc' in md
        except ClientError:
            pass

    return False

def miscfile_meta(path):
    try:
        return json.load(open(miscfile_meta_path(path)))
    except IOError:
        if using_s3:
            return s3.get_key(path).metadata
        else:
            raise

class MiscFile(Base):
    """
    This ORM class is meant to store metadata associated to opaque files, that cannot be associated
    to the search form, summary, etc..
    """

    __tablename__ = 'miscfile'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False, index=True)
    diskfile    = relation(DiskFile, order_by=id)
    release     = Column(DateTime, nullable=False)
    description = Column(Text)
    program_id  = Column(Text, index=True)
