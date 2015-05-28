"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
import json

from orm import sessionfactory
from web.summary import list_headers
from web.standards import xmlstandardobs
import apache_return_codes as apache

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
            if header.phot_standard:
                xmlstandardobs(req, header.id)
            req.write("</file>")
    finally:
        session.close()
    req.write("</file_list>")
    return apache.OK


def jsonfilelist(req, selection):
    """
    This generates a JSON list of the files that met the selection
    """
    req.content_type = "application/json"

    session = sessionfactory()
    orderby = ['filename_asc']
    try:
        headers = list_headers(session, selection, orderby)
        thelist = []
        for header in headers:
            thedict = {}
            thedict['name'] = str(header.diskfile.file.name)
            thedict['filename'] = str(header.diskfile.filename)
            thedict['path'] = str(header.diskfile.path)
            thedict['compressed'] = str(header.diskfile.compressed)
            thedict['size'] = str(header.diskfile.file_size)
            thedict['file_size'] = str(header.diskfile.file_size)
            thedict['data_size'] = str(header.diskfile.data_size)
            thedict['md5'] = str(header.diskfile.file_md5)
            thedict['file_md5'] = str(header.diskfile.file_md5)
            thedict['data_md5'] = str(header.diskfile.data_md5)
            thedict['lastmod'] = str(header.diskfile.lastmod)
            thelist.append(thedict)
    finally:
        session.close()
    json.dump(thelist, req, indent=4)
    return apache.OK

def jsonsummary(req, selection):
    """
    This generates a JSON list of the files that met the selection.
    This contains most of the details from the header table
    """
    req.content_type = "application/json"

    # Like the summaries, only list canonical files by default
    if 'canonical' not in selection.keys():
        selection['canonical']=True

    session = sessionfactory()
    orderby = ['filename_asc']
    try:
        headers = list_headers(session, selection, orderby)
        thelist = []
        for header in headers:
            thedict = {}
            thedict['name'] = str(header.diskfile.file.name)
            thedict['filename'] = str(header.diskfile.filename)
            thedict['path'] = str(header.diskfile.path)
            thedict['compressed'] = str(header.diskfile.compressed)
            thedict['size'] = str(header.diskfile.file_size)
            thedict['file_size'] = str(header.diskfile.file_size)
            thedict['data_size'] = str(header.diskfile.data_size)
            thedict['md5'] = str(header.diskfile.file_md5)
            thedict['file_md5'] = str(header.diskfile.file_md5)
            thedict['data_md5'] = str(header.diskfile.data_md5)
            thedict['lastmod'] = str(header.diskfile.lastmod)
            thedict['program_id'] = str(header.program_id)
            thedict['engineering'] = str(header.engineering)
            thedict['science_verification'] = str(header.science_verification)
            thedict['calibration_program'] = str(header.calibration_program)
            thedict['observation_id'] = str(header.observation_id)
            thedict['data_label'] = str(header.data_label)
            thedict['telescope'] = str(header.telescope)
            thedict['instrument'] = str(header.instrument)
            thedict['ut_datetime'] = str(header.ut_datetime)
            thedict['local_time'] = str(header.local_time)
            thedict['observation_type'] = str(header.observation_type)
            thedict['observation_class'] = str(header.observation_class)
            thedict['object'] = str(header.object)
            thedict['ra'] = str(header.ra)
            thedict['dec'] = str(header.dec)
            thedict['azimuth'] = str(header.azimuth)
            thedict['elevation'] = str(header.elevation)
            thedict['cass_rotator_pa'] = str(header.cass_rotator_pa)
            thedict['airmass'] = str(header.airmass)
            thedict['filter_name'] = str(header.filter_name)
            thedict['exposure_time'] = str(header.exposure_time)
            thedict['disperser'] = str(header.disperser)
            thedict['camera'] = str(header.camera)
            thedict['central_wavelength'] = str(header.central_wavelength)
            thedict['wavelength_band'] = str(header.wavelength_band)
            thedict['focal_plane_mask'] = str(header.focal_plane_mask)
            thedict['detector_binning'] = str(header.detector_binning)
            thedict['detector_config'] = str(header.detector_config)
            thedict['detector_roi_setting'] = str(header.detector_roi_setting)
            thedict['spectroscopy'] = str(header.spectroscopy)
            thedict['mode'] = str(header.mode)
            thedict['adaptive_optics'] = str(header.adaptive_optics)
            thedict['laser_guide_star'] = str(header.laser_guide_star)
            thedict['wavefront_sensor'] = str(header.wavefront_sensor)
            thedict['gcal_lamp'] = str(header.gcal_lamp)
            thedict['raw_iq'] = str(header.raw_iq)
            thedict['raw_cc'] = str(header.raw_cc)
            thedict['raw_wv'] = str(header.raw_wv)
            thedict['raw_bg'] = str(header.raw_bg)
            thedict['requested_iq'] = str(header.requested_iq)
            thedict['requested_cc'] = str(header.requested_cc)
            thedict['requested_wv'] = str(header.requested_wv)
            thedict['requested_bg'] = str(header.requested_bg)
            thedict['qa_state'] = str(header.qa_state)
            thedict['release'] = str(header.release)
            thedict['reduction'] = str(header.reduction)
            thedict['types'] = str(header.types)
            thedict['phot_standard'] = str(header.phot_standard)
            
            thelist.append(thedict)
    finally:
        session.close()
    json.dump(thelist, req, indent=4)
    return apache.OK

