"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
import json

from ..orm.header import Header
from ..orm.diskfile import DiskFile
from ..orm.file import File
from selection import queryselection, openquery
from .summary import list_headers
from .standards import get_standard_obs
from ..apache_return_codes import HTTP_OK

from ..utils.userprogram import canhave_coords, got_magic
from ..utils.web import Context, with_content_type

from . import templating

diskfile_fields = ('filename', 'path', 'compressed', 'file_size',
                   'data_size', 'file_md5', 'data_md5', 'lastmod', 'mdready')

@templating.templated("filelist/filelist.xml", content_type='text/xml', with_generator=True)
def xmlfilelist(req, selection):
    """
    This generates an xml list of the files that met the selection
    """

    def generate_headers(selection):
        orderby = ['filename_asc']
        for header, diskfile, file in list_headers(selection, orderby, full_query=True):
            ret = (header, diskfile, file)
            if header.phot_standard:
                yield ret + (get_standard_obs(req, header.id),)
            else:
                yield ret + (None,)

    return dict(
        selection = selection,
        content   = generate_headers(selection),
        )

def diskfile_dicts(headers, return_header=False):
    for header in headers:
        thedict = {}
        thedict['name'] = _for_json(header.diskfile.file.name)
        for field in diskfile_fields:
            thedict[field] = _for_json(getattr(header.diskfile, field))
        thedict['size'] = thedict['file_size']
        thedict['md5'] = thedict['file_md5']
        if not return_header:
            yield thedict
        else:
            yield thedict, header

@with_content_type('application/json')
def jsonfilelist(req, selection):
    """
    This generates a JSON list of the files that met the selection
    """

    orderby = ['filename_asc']
    headers = list_headers(selection, orderby)
    thelist = list(diskfile_dicts(headers))

    Context().resp.append_json(thelist, indent=4)
    return HTTP_OK

header_fields = ('program_id', 'engineering', 'science_verification',
                 'calibration_program', 'observation_id', 'data_label',
                 'telescope', 'instrument', 'ut_datetime', 'local_time',
                 'observation_type', 'observation_class', 'object', 'ra',
                 'dec', 'azimuth', 'elevation', 'cass_rotator_pa',
                 'airmass', 'filter_name', 'exposure_time', 'disperser',
                 'camera', 'central_wavelength', 'wavelength_band',
                 'focal_plane_mask', 'detector_binning', 'detector_config',
                 'detector_roi_setting', 'spectroscopy', 'mode',
                 'adaptive_optics', 'laser_guide_star', 'wavefront_sensor',
                 'gcal_lamp', 'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg',
                 'requested_iq', 'requested_cc', 'requested_wv',
                 'requested_bg', 'qa_state', 'release', 'reduction',
                 'types', 'phot_standard')

proprietary_fields = ('ra', 'dec', 'azimuth', 'elevation', 'airmass',
                      'object', 'cass_rotator_pa')

@with_content_type('application/json')
def jsonsummary(req, selection):
    """
    This generates a JSON list of the files that met the selection.
    This contains most of the details from the header table

    We have to check for proprietary coordinates here and blank out
    the coordinates if the user does not have access to them.
    """

    ctx = Context()

    # Like the summaries, only list canonical files by default
    if 'canonical' not in selection.keys():
        selection['canonical']=True

    orderby = ['filename_asc']

    # Get the current user if logged id
    user = ctx.user
    gotmagic = got_magic(req)

    headers = list_headers(selection, orderby)
    thelist = []
    for thedict, header in diskfile_dicts(headers, return_header=True):
        chc = canhave_coords(ctx.session, user, header, gotmagic)
        for field in header_fields:
            thedict[field] = _for_json(getattr(header, field))
        if not chc:
            for field in proprietary_fields:
                thedict[field] = None

        thelist.append(thedict)

    if openquery(selection) and thelist:
        thelist[-1]['results_truncated'] = True

    ctx.resp.append_json(thelist, indent=4)
    return HTTP_OK

@with_content_type('application/json')
def jsonqastate(req, selection):
    """
    This generates a JSON list giving datalabel, entrytime, data_md5 and qa_state.
    It is intended for use by the ODB.
    It does not limit the number of results
    """

    ctx = Context()

    # Like the summaries, only list canonical files by default
    if 'canonical' not in selection.keys():
        selection['canonical']=True

    # We do this directly rather than with list_headers for efficiency
    # as this could be used on very large queries bu the ODB
    query = ctx.session.query(Header, DiskFile)
    query = query.filter(Header.diskfile_id == DiskFile.id)
    query = queryselection(query, selection)

    thelist = []
    for header, diskfile in query:
        thelist.append({'data_label': _for_json(header.data_label),
                        'filename': _for_json(diskfile.filename),
                        'data_md5': _for_json(diskfile.data_md5),
                        'entrytime': _for_json(diskfile.entrytime),
                        'qa_state': _for_json(header.qa_state)})

    ctx.resp.append_json(thelist)
    return HTTP_OK

from decimal import Decimal
from datetime import datetime, date, time
from sqlalchemy import Integer, Text, DateTime, Numeric, Boolean, Date, Time, BigInteger, Enum
def _for_json(thing):
    """
    JSON can't serialize some types, this does best representation we can
    """
    if isinstance(thing, (Text, datetime, date, time, DateTime, Date, Time)):
        return str(thing)
    if isinstance(thing, (Integer, BigInteger)):
        return int(thing)
    if isinstance(thing, (Numeric, Decimal)):
        return float(thing)
    if isinstance(thing, Enum):
        return str(thing)
    return thing
