"""
This module contains the functions for curation_report.py that compare items in
the tables Header and DiskFile.

"""
from .diskfile import DiskFile
from .file     import File

from sqlalchemy import text, distinct
from sqlalchemy.orm import aliased

diskfile_alias = aliased(DiskFile)

def duplicate_canonicals(session):
    """Find canonical DiskFiles with duplicate file_ids.

       Returns a query object"""
    # Make an alias of DiskFile
    # Self join DiskFile with its alias and compare their file_ids
    return (
        session.query(distinct(DiskFile.id), File)
                .join(File)
                .join(diskfile_alias, DiskFile.file_id == diskfile_alias.file_id)
                .filter(DiskFile.id != diskfile_alias.id)
                .filter(DiskFile.canonical == True)
                .filter(diskfile_alias.canonical == True)
                .order_by(DiskFile.id)
        )
    return diskfiles


def duplicate_present(session):
    """Find present DiskFiles with duplicate file_ids.

       Returns a query object"""

    return (
        session.query(distinct(DiskFile.id), File)
            .join(File)
            .join(diskfile_alias, DiskFile.file_id == diskfile_alias.file_id)
            .filter(DiskFile.id != diskfile_alias.id)
            .filter(DiskFile.present == True)
            .filter(diskfile_alias.present == True)
            .order_by(DiskFile.id)
        )

def present_not_canonical(session):
    """Find present DiskFiles that are not canonical.

       Returns a query object"""

    return (
        session.query(distinct(DiskFile.id), File)
            .join(File)
            .filter(DiskFile.present == True)
            .filter(DiskFile.canonical == False)
            .order_by(DiskFile.id)
        )
