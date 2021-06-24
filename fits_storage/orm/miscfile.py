from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import relation

from gemini_obs_db import Base, NoResultFound
from gemini_obs_db.diskfile import DiskFile

from ..fits_storage_config import using_s3, upload_staging_path

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
    """
    Prepar a filename for use as a `miscfile`.  This is done by adding
    a prefix of `miscfile_` to the filename, if it not already there

    Parameters
    ----------
    filename : str
        String filename to normalize

    Returns
    -------
    str : Name of the file with an ensured `miscfile_` prefix
    """
    if not filename.startswith(MISCFILE_PREFIX):
        return MISCFILE_PREFIX + filename

    return filename


def miscfile_meta_path(path):
    """
    Derive the path to the JSON file with metadata for the `miscfile`

    Parameters
    ----------
    path : str
        Path to derive metadata filename from

    Returns
    -------
    str : path to JSON metadata file
    """
    return os.path.join(upload_staging_path, path + '.json')


def is_miscfile(path):
    """
    Check if the given path is a `miscfile`

    Parameters
    ----------
    path : str
        Path to the file to check

    Returns
    -------
    bool : True if file is a `miscfile`
    """
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
    """
    Read the description from the passed metadata

    Parameters
    ----------
    meta : dict
        Metadata dictionary to read description from

    Returns
    -------
        str : Description decoded from `meta` dictionary
    """
    try:
        return decode_string(meta['description'])
    except (KeyError, AttributeError):
        # If there's no description member, or it is None, pass
        pass


def miscfile_meta(path, urlencode=False):
    """
    Read `miscfile` metadata for the given path

    Parameters
    ----------
    path : str
        Path to miscfile to extract metadata for (note: NOT the path to the JSON)
    urlencode : bool
        If True, we do urlencoding on the description

    Returns
    -------
    dict : Metadata dictionary
    """
    try:
        meta = json.load(io.open(miscfile_meta_path(path), encoding='utf-8'))
        if urlencode:
            meta['description'] = encode_string(meta['description'].encode('utf-8')) \
                .decode(encoding='utf-8', errors='ignore')
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
