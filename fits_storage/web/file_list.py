"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
import json

from ..orm import sessionfactory
from ..orm.header import Header
from ..orm.diskfile import DiskFile
from selection import queryselection
from .summary import list_headers
from .standards import xmlstandardobs
from ..apache_return_codes import HTTP_OK

from ..utils.userprogram import canhave_coords, got_magic
from .user import userfromcookie

diskfile_fields = ('filename', 'path', 'compressed', 'file_size',
                   'data_size', 'file_md5', 'data_md5', 'lastmod', 'mdready')

def xmlfilelist(req, selection):
    """
    This generates an xml list of the files that met the selection
    """
    req.content_type = "text/xml"
    req.write('<?xml version="1.0" ?>')
    req.write("<file_list>")
    req.write("<selection>%s</selection>" % selection)

    session = sessionfactory()
    orderby = ['filename_asc']
    try:
        headers = list_headers(session, selection, orderby)
        for header in headers:
            req.write("<file>")
            req.write("<name>%s</name>" % header.diskfile.file.name)
            req.write("<filename>%s</filename>" % header.diskfile.filename)
            req.write("<path>%s</path>" % header.diskfile.path)
            req.write("<compressed>%s</compressed>" % header.diskfile.compressed)
            req.write("<size>%d</size>" % header.diskfile.file_size)
            req.write("<file_size>%d</file_size>" % header.diskfile.file_size)
            req.write("<data_size>%d</data_size>" % header.diskfile.data_size)
            req.write("<md5>%s</md5>" % header.diskfile.file_md5)
            req.write("<file_md5>%s</file_md5>" % header.diskfile.file_md5)
            req.write("<data_md5>%s</data_md5>" % header.diskfile.data_md5)
            req.write("<lastmod>%s</lastmod>" % header.diskfile.lastmod)
            req.write("<mdready>%s</mdready>" % header.diskfile.mdready)
            if header.phot_standard:
                xmlstandardobs(req, header.id)
            req.write("</file>")
    finally:
        session.close()
    req.write("</file_list>")
    return HTTP_OK

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

def jsonfilelist(req, selection):
    """
    This generates a JSON list of the files that met the selection
    """
    req.content_type = "application/json"

    session = sessionfactory()
    orderby = ['filename_asc']
    try:
        headers = list_headers(session, selection, orderby)
        thelist = list(diskfile_dicts(headers))
    finally:
        session.close()
    json.dump(thelist, req, indent=4)
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

def jsonsummary(req, selection):
    """
    This generates a JSON list of the files that met the selection.
    This contains most of the details from the header table

    We have to check for proprietary coordinates here and blank out
    the coordinates if the user does not have access to them.
    """
    req.content_type = "application/json"

    # Like the summaries, only list canonical files by default
    if 'canonical' not in selection.keys():
        selection['canonical']=True

    session = sessionfactory()
    orderby = ['filename_asc']

    # Get the current user if logged id
    user = userfromcookie(session, req)
    gotmagic = got_magic(req)

    try:
        headers = list_headers(session, selection, orderby)
        thelist = []
        for thedict, header in diskfile_dicts(headers, return_header=True):
            chc = canhave_coords(session, user, header, gotmagic)
            for field in header_fields:
                thedict[field] = _for_json(getattr(header, field))
            if not chc:
                for field in proprietary_fields:
                    thedict[field] = None

            thelist.append(thedict)
    finally:
        session.close()
    json.dump(thelist, req, indent=4)
    return HTTP_OK

def jsonqastate(req, selection):
    """
    This generates a JSON list giving datalabel and qa_state.
    It is intended for use by the ODB.
    It does not limit the number of results
    """
    req.content_type = "application/json"
    # Like the summaries, only list canonical files by default
    if 'canonical' not in selection.keys():
        selection['canonical']=True

    session = sessionfactory()
    try:
       # We do this directly rather than with list_headers for efficiency
       # as this could be used on very large queries bu the ODB
       query = session.query(Header).select_from(Header, DiskFile)
       query = query.filter(Header.diskfile_id == DiskFile.id) 
       query = queryselection(query, selection)

       thelist = []
       for header in query:
           thelist.append({'data_label': _for_json(header.data_label),
                           'qa_state': _for_json(header.qa_state)})
    finally:
        session.close()

    json.dump(thelist, req)
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
