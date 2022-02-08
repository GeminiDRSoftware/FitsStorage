import datetime

from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import relation, relationship

from gemini_obs_db.db import Base

from ..fits_storage_config import using_s3, upload_staging_path
from ..utils.web import get_context

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
    file_children = relationship("MiscFilePlus")
    folder_children = relationship("MiscFileFolder", overlaps="folder")

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

    def parentpath(self):
        if self.folder:
            return self.folder.path()
        else:
            return ''

    def linkpath(self):
        def urlify(folder):
            return f"<a href=\"/miscfilesplus/browse/{folder.collection.name}/{folder.path()}/\">{folder.name}</a>"
        if self.folder:
            return self.folder.linkpath() + "/" + urlify(self)
        else:
            return urlify(self)

    def _maybe_initialize_composites(self):
        # Note: this odd approach is because __init__ doesn't mesh well with SQLAlchemy.  So we just
        # hasattr() here and lazy init the cached composite values if needed
        if not hasattr(self, '_composites_initialized') or not self._composites_initialized:
            # self._release = None
            self._last_modified = None
            self._size = 0
            for folder in self.folder_children:
                folder_rd = folder.release()
                if self._release is None or (folder_rd is not None and folder_rd > self._release):
                    self._release = folder_rd
                folder_lm = folder.last_modified()
                if self._last_modified is None or (folder_lm is not None and folder_lm > self._last_modified):
                    self._last_modified = folder_lm
                self._size += folder.size()
            for file in self.file_children:
                # file_rd = file.release
                # if self._release is None or (file_rd is not None and file_rd > self._release):
                #     self._release = file_rd
                file_lm = file.last_modified
                if self._last_modified is None or (file_lm is not None and file_lm > self._last_modified):
                    self._last_modified = file_lm
                self._size += file.size

            self._composites_initialized = True

    # def release(self):
    #     self._maybe_initialize_composites()
    #     return self._release

    def last_modified(self):
        self._maybe_initialize_composites()
        return self._last_modified

    def size(self):
        self._maybe_initialize_composites()
        return self._size


class MiscFilePlus(Base):
    """
    This ORM class holds data for an individual file hosted in
    the MiscFilePlus system.

    The collection should match the parent folder collection, if present.
    """
    __tablename__ = 'miscfile_plus'

    id            = Column(Integer, primary_key=True)
    folder_id     = Column(Integer, ForeignKey('miscfile_folder.id'), nullable=True, index=True)
    folder        = relation(MiscFileFolder, order_by=id, overlaps="file_children")
    collection_id = Column(Integer, ForeignKey('miscfile_collection.id'), nullable=True, index=True)
    collection    = relation(MiscFileCollection, order_by=id)
    release       = Column(DateTime, nullable=False)
    description   = Column(Text)
    program_id    = Column(Text, index=True)
    filename      = Column(Text, index=True)
    size          = Column(Integer, index=True)
    last_modified = Column(DateTime, index=True)

    def parentpath(self):
        if self.folder:
            return self.folder.path()
        else:
            return ''

    def check_download_permission(self):
        if get_context().is_staffer:
            return True
        if get_context().req.user in self.collection.users:
            return True
        if self.release <= datetime.datetime.utcnow():
            return True
        return False
