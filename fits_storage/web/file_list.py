"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
import json

from ..orm.header import Header
from ..orm.diskfile import DiskFile
from ..orm.file import File
from .selection import queryselection, openquery
from .summary import list_headers
from .standards import get_standard_obs

from ..utils.userprogram import canhave_coords
from ..utils.web import get_context, with_content_type

from . import templating

from ..logger import logger

diskfile_fields = ('filename', 'path', 'compressed', 'file_size',
                   'data_size', 'file_md5', 'data_md5', 'lastmod', 'mdready',
                   'entrytime')

@templating.templated("filelist/filelist.xml", content_type='text/xml', with_generator=True)
def xmlfilelist(selection):
    """
    This generates an xml list of the files that met the selection
    """

    def generate_headers(selection):
        orderby = ['filename_asc']
        for header, diskfile, file, obslogcomm in list_headers(selection, orderby, full_query=True):
            ret = (header, diskfile, file)
            if header.phot_standard:
                yield ret + (get_standard_obs(header.id),)
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

def jsonfilelist(selection, fields=None):
    """
    This generates a JSON list of the files that met the selection
    """

    orderby = ['filename_asc']
    headers = list_headers(selection, orderby)
    thelist = list(diskfile_dicts(headers))

    if fields is None:
        get_context().resp.send_json(thelist, indent=4)
    else:
        get_context().resp.send_json([dict((k, d[k]) for k in fields) for d in thelist], indent=4)

header_fields = ('program_id', 'engineering', 'science_verification',
                 'calibration_program', 'observation_id', 'data_label',
                 'telescope', 'instrument', 'ut_datetime', 'local_time',
                 'observation_type', 'observation_class', 'object', 'ra',
                 'dec', 'azimuth', 'elevation', 'cass_rotator_pa',
                 'airmass', 'filter_name', 'exposure_time', 'disperser',
                 'camera', 'central_wavelength', 'wavelength_band',
                 'focal_plane_mask', 'detector_binning', 'detector_gain_setting',
                 'detector_roi_setting', 'detector_readspeed_setting',
                 'detector_welldepth_setting', 'detector_readmode_setting', 
                 'spectroscopy', 'mode',
                 'adaptive_optics', 'laser_guide_star', 'wavefront_sensor',
                 'gcal_lamp', 'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg',
                 'requested_iq', 'requested_cc', 'requested_wv',
                 'requested_bg', 'qa_state', 'release', 'reduction',
                 'types', 'phot_standard')

proprietary_fields = ('ra', 'dec', 'azimuth', 'elevation', 'airmass',
                      'object', 'cass_rotator_pa')

def jsonsummary(selection):
    """
    This generates a JSON list of the files that met the selection.
    This contains most of the details from the header table

    We have to check for proprietary coordinates here and blank out
    the coordinates if the user does not have access to them.
    """

    ctx = get_context()

    # Like the summaries, only list canonical files by default
    if 'canonical' not in list(selection.keys()):
        selection['canonical']=True

    orderby = ['filename_asc']

    # Get the current user if logged id
    user = ctx.user
    gotmagic = ctx.got_magic

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

    ctx.resp.send_json(thelist, indent=4)

def jsonqastate(selection):
    """
    This generates a JSON list giving datalabel, entrytime, data_md5 and qa_state.
    It is intended for use by the ODB.
    It does not limit the number of results
    """

    ctx = get_context()

    # Like the summaries, only list canonical files by default
    if 'canonical' not in list(selection.keys()):
        selection['canonical']=True

    logger.warn("Entered jsonqastate, selection: %s" % selection)

    # We do this directly rather than with list_headers for efficiency
    # as this could be used on very large queries bu the ODB
    query = ctx.session.query(Header, DiskFile).select_from(Header, DiskFile, File)
    query = query.filter(Header.diskfile_id == DiskFile.id)
    query = query.filter(DiskFile.file_id == File.id)
    query = queryselection(query, selection)

    logger.warn("Starting query for jsonqastate")

    thelist = []
    for header, diskfile in query:
        thelist.append({'data_label': _for_json(header.data_label),
                        'filename': _for_json(diskfile.filename),
                        'data_md5': _for_json(diskfile.data_md5),
                        'entrytime': _for_json(diskfile.entrytime),
                        'qa_state': _for_json(header.qa_state)})

    logger.warn("Query finished, sending json")

    ctx.resp.send_json(thelist)

    logger.warn("Done sending json")

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
