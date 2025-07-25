from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import relationship

from fits_storage.core.orm import Base
from fits_storage.core.orm.diskfile import DiskFile

from fits_storage.config import get_config
fsc = get_config()

if fsc.using_s3:
    from ..aws_s3 import Boto3Helper, ClientError
    s3 = Boto3Helper()

import json
import os
import dateutil.parser
from base64 import urlsafe_b64encode as encode_string
from base64 import urlsafe_b64decode as decode_string


class FileClash(Exception):
    pass


MISCFILE_PREFIX = 'miscfile_'


def normalize_diskname(filename):
    """
    Prepare a filename for use as a `miscfile`.  This is done by adding
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
    fsc = get_config()
    return os.path.join(fsc.upload_staging_dir, path + '.json')


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
    fsc = get_config()
    if os.path.exists(miscfile_meta_path(path)):
        return True
    elif fsc.using_s3:
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
    Read `miscfile` metadata from json file corresponding to the miscfile
    at the given path

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
    fsc = get_config()
    try:
        meta = json.load(open(miscfile_meta_path(path), encoding='utf-8'))
        if urlencode:
            meta['description'] = encode_string(meta['description'].encode('utf-8')) \
                .decode(encoding='utf-8', errors='ignore')
    except IOError:
        if fsc.using_s3:
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
    diskfile_id = Column(Integer, ForeignKey('diskfile.id'), nullable=False,
                         index=True)
    diskfile    = relationship(DiskFile, order_by=id)
    release     = Column(DateTime, nullable=False)
    description = Column(Text)
    program_id  = Column(Text, index=True)

    def __init__(self, diskfile, parse_meta=True):
        """
        Create a new miscfile, pass diskfile_id and meta dict
        """

        self.diskfile_id = diskfile.id
        if parse_meta:
            meta = miscfile_meta(diskfile.filename)
            self.release = dateutil.parser.parse(meta['release'])
            self.program_id = meta['program']

            # self.description = meta['description']
            desc = meta['description']
            if isinstance(desc, str):
                if desc.startswith('\\x'):
                    desc = bytes.fromhex(desc[2:].replace('\\x', ''))\
                        .decode('utf-8')
            else:
                desc_bytes = desc
                desc = desc_bytes.decode('utf-8', errors='ignore')
            self.description = desc
