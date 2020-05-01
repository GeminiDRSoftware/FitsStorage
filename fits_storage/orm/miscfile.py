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
import io
from base64 import urlsafe_b64encode as encode_string
from base64 import urlsafe_b64decode as decode_string

# ------------------------------------------------------------------------------
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

def decode_description(meta):
    try:
        return decode_string(meta['description'])
    except (KeyError, AttributeError):
        # If there's no description member, or it is None, pass
        pass

def miscfile_meta(path, urlencode=False):
    try:
        meta = json.load(io.open(miscfile_meta_path(path), encoding='utf-8'))
        if urlencode:
            meta['description'] = encode_string(meta['description'].encode('utf-8'))
    except IOError:
        if using_s3:
            meta = s3.get_key(path).metadata
            meta['description'] = decode_description(meta)
        else:
            raise

    return meta

class MiscFile(Base):
    """
    This ORM class is meant to store metadata associated to opaque files, 
    that cannot be associated to the search form, summary, etc..

    """
    __tablename__ = 'miscfile'

    id = Column(Integer, primary_key=True)
    diskfile_id = Column(Integer,ForeignKey('diskfile.id'), nullable=False, index=True)
    diskfile    = relation(DiskFile, order_by=id)
    release     = Column(DateTime, nullable=False)
    description = Column(Text)
    program_id  = Column(Text, index=True)
