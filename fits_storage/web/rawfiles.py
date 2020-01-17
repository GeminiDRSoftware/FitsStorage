"""
This module contains the calibrations html generator function.
"""
import datetime
from ..orm import sessionfactory
from .selection import sayselection, queryselection, openquery
from ..cal import get_cal_object
from ..fits_storage_config import fits_servername, fits_system_status, use_as_archive

from ..orm.header import Header
from ..orm.diskfile import DiskFile
from ..orm.file import File
from ..orm.provenance import Provenance

from ..utils.web import get_context

from . import templating

from sqlalchemy import join, desc, or_
from sqlalchemy.orm import aliased


class RowYielder:
    """
    Instances of this class are used by the summary template to iterate over the
    rows of data.
    """
    def __init__(self, rows):
        self.rows = iter(rows)

    def __iter__(self):
        return self

    def __next__(self):
        "Obtain the next row of data and keep some stats about it."
        row = next(self.rows)
        provenance = row[0]
        diskfile = row[1]
        if diskfile is not None and diskfile.provenance:
            has_raw = True
        else:
            has_raw = False
        data = {
            "timestamp": provenance.timestamp,
            "filename": provenance.filename,
            "md5": provenance.md5,
            "primitive": provenance.primitive,
            "has_raw": has_raw
        }
        return data


@templating.templated("rawfiles.html", with_generator=True)
def rawfiles(filename):
    """
    Return all raw files related to the given file from it's provenance records.
    
    This returns a summary of the input files from this file's provenance that
    went into creating it, along with the timestamp, their md5 at the time and
    the primitive they were used in.  If the input file itself has provenance
    data available, that is also indicated.  This allows us to generate links
    in the web UI to further drill down into their provenance without having a
    bunch of confusing dead ends for the majority that have nothing.
    """
    input_file = aliased(DiskFile, name='input_file')
    query = get_context().session.query(Provenance, input_file) \
        .join(DiskFile, DiskFile.id == Provenance.diskfile_id) \
        .outerjoin(input_file, input_file.filename == Provenance.filename) \
        .filter(DiskFile.filename == filename) \
        .filter(DiskFile.canonical == True) \
        .filter(or_(input_file.canonical == True, input_file.canonical == None))
    query = query.order_by(Provenance.timestamp)

    template_args = dict(
        filename    = filename,
        data_rows   = RowYielder(query),
    )

    return template_args
