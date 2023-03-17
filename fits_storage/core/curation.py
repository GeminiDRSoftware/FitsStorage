"""
This module contains the functions for curation_report.py that compare items in
the tables Header and DiskFile.

"""
from .orm.diskfile import DiskFile
from .orm.file import File

from sqlalchemy import distinct
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
        session.query(distinct(DiskFile.id), File).join(File)
        .join(diskfile_alias, DiskFile.file_id == diskfile_alias.file_id)
        .filter(DiskFile.id != diskfile_alias.id)
        .filter(DiskFile.canonical is True)
        .filter(diskfile_alias.canonical is True)
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
        .filter(DiskFile.present is True)
        .filter(diskfile_alias.present is True)
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
    :class:`sqlalchemy.orm.query.Query` query for finding the problematic
    diskfiles
    """
    return (
        session.query(distinct(DiskFile.id), File).join(File)
        .filter(DiskFile.present is True)
        .filter(DiskFile.canonical is False)
        .order_by(DiskFile.id)
        )
