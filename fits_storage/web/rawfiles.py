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

from sqlalchemy import join, desc


class RowYielder:
    """
    Instances of this class are used by the summary template to iterate over the
    rows of data.

    These rows of data could be accessed directly, but there are a number of things
    to compute (total size, total downloadable files, etc.). This task is better
    done in the controller side of the process (this class), instead of in the
    view (template)

    The instance consumes its source data and once it has iterated over all the
    available header objects, it can only be used to query the totalized values.
    """
    def __init__(self, provenance):
        self.provenance = iter(provenance)

    def __iter__(self):
        return self

    def __next__(self):
        "Obtain the next row of data and keep some stats about it."
        provenance = next(self.provenance)
        row = provenance
        # add row "type" to support tab-differentiation in our template
        return row


@templating.templated("rawfiles.html", with_generator=True)
def rawfiles(filename):
    """
    This is the calibrations generator. It implements a human readable calibration association server.
    This is mostly used by the Gemini SOSs to detect missing calibrations, and it defaults to the 
    SOS required calibrations policy.
    """
    counter = {
        'warnings': 0,
        'missings': 0,
    }


    provenance_query = get_context().session.query(Provenance)
    # OK, find the target files
    # The Basic Query
    #query = get_context().session.query(Header).select_from(join(join(DiskFile, File), Header))

    # Only the canonical versions
    #selection['canonical'] = True

    #query = queryselection(query, selection)

    # If openquery, decline to do it
    # if openquery(selection):
    #     template_args['is_open'] = True
    #     return template_args

    # Knock out the FAILs
    # Knock out ENG programs
    # Disregard SV-101. This is an undesirable hardwire
    # Order by date, most recent first
    # headers = query.filter(Header.qa_state != 'Fail')\
    #                .filter(Header.engineering != True)\
    #                .filter(~Header.program_id.like('%SV-101%'))\
    #                .order_by(desc(Header.ut_datetime))

    query = get_context().session.query(Provenance).select_from(join(DiskFile, Provenance))
    query = query.filter(DiskFile.filename == filename)
    query.order_by(Provenance.timestamp)

    template_args = dict(
        filename    = filename,
        data_rows   = RowYielder(query),
    )

    return template_args
