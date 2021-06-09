from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import relation, relationship

from . import Base, NoResultFound
from .diskfile import DiskFile

from ..fits_storage_config import using_s3, upload_staging_path

if using_s3:
    from ..utils.aws_s3 import get_helper, ClientError
    s3 = get_helper()

import json
import os
import io
from base64 import urlsafe_b64encode as encode_string
from base64 import urlsafe_b64decode as decode_string


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


""" Association table for mapping write-access users to a collection. """
MiscFileCollectionUsers = Table('miscfile_collection_users', Base.metadata,
                                Column('collection_id', Integer, ForeignKey('miscfile_collection.id')),
                                Column('user_id', Integer, ForeignKey('archiveuser.id')))


class MiscFileCollection(Base):
    """
    This ORM class is meant to store metadata associated to opaque files,
    that cannot be associated to the search form, summary, etc..

    """
    __tablename__ = 'miscfile_collection'

    id = Column(Integer, primary_key=True)
    name          = Column(Text, index=True)
    description   = Column(Text, index=True)
    program_id    = Column(Text, index=True)
    users = relationship('User', secondary=MiscFileCollectionUsers)


class MiscFileFolder(Base):
    """
    This ORM class is meant to track a folder in the MiscFilePlus system.

    The collection should match the parent collection.  If the parent
    folder is None, this is a top-level folder.
    """
    __tablename__ = 'miscfile_folder'

    id            = Column(Integer, primary_key=True)
    name          = Column(Text, index=True)
    folder_id     = Column(Integer, ForeignKey('miscfile_folder.id'), nullable=True, index=True)
    folder        = relation('MiscFileFolder', remote_side=[id])
    collection_id = Column(Integer, ForeignKey('miscfile_collection.id'), nullable=True, index=True)
    collection    = relation(MiscFileCollection, order_by=id)
    release       = Column(DateTime, nullable=False)
    description   = Column(Text)
    program_id    = Column(Text, index=True)

    def path(self):
        """
        Derive the path of this folder as / delimited
        """
        retval = [self.name]
        f = self.folder
        while f is not None:
            retval.insert(0, f.name)
            f = f.folder
        return '/'.join(retval)


class MiscFilePlus(Base):
    """
    This ORM class holds data for an individual file hosted in
    the MiscFilePlus system.

    The collection should match the parent folder collection, if present.
    """
    __tablename__ = 'miscfile_plus'

    id            = Column(Integer, primary_key=True)
    folder_id     = Column(Integer, ForeignKey('miscfile_folder.id'), nullable=True, index=True)
    folder        = relation(MiscFileFolder, order_by=id)
    collection_id = Column(Integer, ForeignKey('miscfile_collection.id'), nullable=True, index=True)
    collection    = relation(MiscFileCollection, order_by=id)
    release       = Column(DateTime, nullable=False)
    description   = Column(Text)
    program_id    = Column(Text, index=True)
    filename      = Column(Text, index=True)
    size          = Column(Integer, index=True)
    last_modified = Column(DateTime, index=True)
