"""
This module contains the functions for curation_report.py that compare items in
the tables Header and DiskFile.

"""
from gemini_obs_db.diskfile import DiskFile
from gemini_obs_db.file     import File

from sqlalchemy import text, distinct
from sqlalchemy.orm import aliased

diskfile_alias = aliased(DiskFile)


def duplicate_canonicals(session):
    """
    Find canonical DiskFiles with duplicate file_ids.

    Parameters
    ----------
    session: :class:`sqlalchemy.orm.session.Session`
        SQL Alchemy session to query for duplicates

    Returns
    -------
    :class:`sqlalchemy.orm.query.Query` query for finding the duplicates
    """
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


def duplicate_present(session):
    """
    Find present DiskFiles with duplicate file_ids.

    Parameters
    ----------
    session: :class:`sqlalchemy.orm.session.Session`
        SQL Alchemy session to query for duplicates

    Returns
    -------
    :class:`sqlalchemy.orm.query.Query` query for finding the duplicates
    """
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
    """
    Find present DiskFiles that are not canonical.

    Parameters
    ----------
    session: :class:`sqlalchemy.orm.session.Session`
        SQL Alchemy session to query for present non-canonical files

    Returns
    -------
    :class:`sqlalchemy.orm.query.Query` query for finding the problematic diskfiles
    """
    return (
        session.query(distinct(DiskFile.id), File)
            .join(File)
            .filter(DiskFile.present == True)
            .filter(DiskFile.canonical == False)
            .order_by(DiskFile.id)
        )
