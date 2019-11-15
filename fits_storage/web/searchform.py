"""
This is the searchform module.

"""
import os
import json
import urllib.request, urllib.parse, urllib.error
import contextlib

from xml.dom import minidom
from xml.parsers.expat import ExpatError

from . import templating

from .calibrations import calibrations
from ..gemini_metadata_utils import GeminiDataLabel, GeminiObservation

from .selection import getselection, selection_to_URL
from .summary import summary_body
from .summary_generator import selection_to_column_names, selection_to_form_indices
from .summary_generator import formdata_to_compressed, search_col_mapping

from ..fits_storage_config import fits_aux_datadir, fits_servertitle, use_as_archive
from ..utils.web import get_context, Return

# ------------------------------------------------------------------------------
@templating.templated("search_and_summary/searchform.html", with_generator=True)
def searchform(things, orderby):
    """
    Generate the searchform html and handle the form submit.

    """
    # How (we think) this (will) all work(s)
    # User gets/posts the url, may or may not have selection criteria on it
    # We parse the url, and create an initial selection dictionary
    # (which may or may not be empty).
    # We parse the formdata and modify the selection dictionary if there was any
    # If there was formdata:
    #    Build a URL from the selection dictionary
    #    Clear the formdata from the request object
    #    Re-direct the user to the new URL (without any formdata)
    # Pre-populate the form fields with what is now in our selection dictionary
    #  by modifying the form html server side before we send it out
    # Send out the form html
    # Send out the results html in-line with that, no ajax or anything
    # User messes with input fields
    # User hits submit - back to top

    ctx = get_context()

    # grab the string version of things before getselection() as that modifies the list.
    thing_string = '/' + '/'.join(things)
    selection = getselection(things)
    formdata = ctx.get_form_data()

    # Also args to pass on to results page
    args_string = ""
    if orderby:
        args_string = '?orderby=%s' % orderby[0]

    column_selection = {}

    if formdata:
        if ((len(formdata) == 6) and
            ('engineering' in list(formdata.keys())) and (formdata['engineering'].value == 'EngExclude') and
            ('science_verification' in list(formdata.keys())) and (formdata['science_verification'].value == 'SvInclude') and
            ('qa_state' in list(formdata.keys())) and (formdata['qa_state'].value == 'NotFail') and
            ('col_selection' in list(formdata.keys())) and
            ('site_monitoring' in list(formdata.keys())) and (formdata['site_monitoring'].value == 'SmExclude') and
            ('Search' in list(formdata.keys())) and (formdata['Search'].value == 'Search')):
            # This is the default form state, someone just hit submit without doing anything.
            pass
        elif list(formdata.keys()) == ['orderby']:
            # All we have is an orderby - don't redirect
            pass
        else:
            # Populate selection dictionary with values from form input
            updateselection(formdata, selection)
            # This logs the seleciton to the apache error log for debugging.
            # ctx.req.log(str(selection))
            # build URL
            urlstring = selection_to_URL(selection, with_columns=True)

            # The following will redirect to some other page. Redirects work by
            # raising an exception, meaning that there's no need for return
            if 'ObsLogsOnly' in list(formdata.keys()):
                # ObsLogs Only search
                ctx.resp.redirect_to('/obslogs' + urlstring)
            elif 'ProgramsOnly' in list(formdata.keys()):
                # Program info only search
                ctx.resp.redirect_to('/programs' + urlstring)
            else:
                # Regular data search
                # clears formdata, refreshes page with updated selection from form
                # TODO: Ask Paul why are we clearing the form data here. Does it make sense?
                # formdata.clear()
                ctx.resp.redirect_to('/searchform' + urlstring + args_string)

    try:
        indices = selection_to_form_indices(selection)
        column_selection = dict((k, k in indices) for k in search_col_mapping)
    except KeyError:
        pass

    # Construct suffix to html title
    things = []
    for thing in ['program_id', 'inst', 'date', 'daterange']:
        if thing in selection:
            things.append(selection[thing])
    title_suffix = ' '.join(things)

    template_args = dict(
        server_title = fits_servertitle,
        title_suffix = title_suffix,
        archive_style = use_as_archive,
        thing_string = thing_string,
        args_string  = args_string,
        updated      = updateform(selection),
        debugging    = False, # Enable this to show some debugging data
        selection    = selection,
        col_sel      = column_selection,
        # Look at the end of the file for this
        **dropdown_options
        )
    if selection:
        template_args.update(summary_body('customsearch', selection, orderby,
                                          additional_columns=selection_to_column_names(selection)))

    return template_args

std_gmos_fpm = {'NS2.0arcsec', 'IFU-R', 'focus_array_new', 'Imaging', '2.0arcsec',
                'NS1.0arcsec', 'NS0.75arcsec', '5.0arcsec', '1.5arcsec', 'IFU-2',
                'NS1.5arcsec', '0.75arcsec', '1.0arcsec', '0.5arcsec'}

def updateform(selection):
    """
    Receives html page as a string and updates it according to values in the 
    selection dictionary. Pre-populates input fields with said selection values.

    """
    dct = {}
    for key, value in list(selection.items()):
        if key in {'program_id', 'observation_id', 'data_label'}:
            # Program id etc
            # don't do program_id if we have already done obs_id, etc
            if key == 'program_id' and not ('observation_id' in selection or 'data_label' in selection):
                dct['program_id'] = value
            if key == 'observation_id' and not ('data_label' in selection):
                dct['program_id'] = value
            if key == 'data_label':
                dct['program_id'] = value

        elif key in {'date', 'daterange'}:
            # If there is a date and a daterange, only use the date part
            if 'date' in selection and 'daterange' in selection:
                key = 'date'
            dct['date'] = value

        elif key == 'spectroscopy' and 'mode' not in selection:
            dct['mode'] = 'spectroscopy' if value else 'imaging'

        elif key == 'engineering':
            if value is True:
                dct[key] = 'EngOnly'
            elif value is False:
                dct[key] = 'EngExclude'
            else:
                dct[key] = 'EngInclude'

        elif key == 'site_monitoring':
            if value is True:
                dct[key] = 'SmInclude'
            else:
                dct[key] = 'SmExclude'

        elif key == 'science_verification':
            if value is True:
                dct[key] = 'SvOnly'
            elif value is False:
                dct[key] = 'SvExclude'
            else:
                dct[key] = 'SvInclude'

        elif key == 'focal_plane_mask':
            if selection.get('inst', '').startswith('GMOS') and value not in std_gmos_fpm:
                # Custom mask name
                dct[key] = 'custom'
                dct['custom_mask'] = value
            else:
                dct[key] = value

        elif value in {'AO', 'NOTAO', 'NGS', 'LGS'}:
            # The Adaptive Optics ends up in various selection keys...
            dct['ao'] = value
        elif key == 'gain':
            # GMOSes are the only thing with a gain field currently
            dct['gmos_gain'] = value
        elif key == 'readspeed':
            # GMOSes are the only thing with a read speed field currently
            dct['gmos_speed'] = value
        elif key == 'readmode':
            if value in ('NodAndShuffle', 'Classic'):
                # For GMOS, this indicates nod and shuffle
                dct['nod_and_shuffle'] = value
            elif value in ('High_Background', 'Medium_Background', 'Low_Background'):
                # NIRI readmode
                dct['niri_readmode'] = value
            elif value in ('Bright_Object', 'Medium_Object', 'Faint_Object'):
                # NIFS readmode
                dct['nifs_readmode'] = value
            elif value in ('Very_Bright_Objects', 'Bright_Objects', 'Faint_Objects', 'Very_Faint_Objects'):
                # GNIRS readmode
                dct['gnirs_readmode'] = value
        elif key == 'welldepth':
                # Only GNIRS has well depth
                dct['gnirs_depth'] = value
        elif key == 'pre_image':
            if value:
                dct['preimage'] = 'preimage'
        else:
            # The rest needs no special processing. This does all the generic
            # pulldown menus and text fields,
            # if key in {'ra', 'dec', 'sr', 'object', 'cenwlen', 'filepre',
            # 'mode', 'filter', 'exposure_time', 'coadds', 'disperser', ...}:
            dct[key] = value

    return dct

def updateselection(formdata, selection):
    """
    Updates the selection dictionary with user input values in formdata
    Handles many different specific cases
    """
    # Populate selection dictionary with values from form input
    for key in formdata:
        # if we got a list, there are multiple fields with that name. This is
        # true for filter at least. Pick the last one (except for col_selection).
        if type(formdata[key]) is list and key != 'col_selection':
            value = formdata[key][-1].value
        if key == 'col_selection':
            value = [x.value for x in formdata[key]]
        else:
            value = formdata[key].value
        if key == 'program_id':
            # if the string starts with progid= then trim that off
            if value[:7] == 'progid=':
                value = value[7:]

            # Ensure it's upper case
            value = value.upper()

            # accepts program id along with observation id and data label for program_id input
            # see if it is an obsid or data label, otherwise treat as program id
            go = GeminiObservation(value)
            dl = GeminiDataLabel(value)

            if go.observation_id:
                selection['observation_id'] = value
            elif dl.datalabel:
                selection['data_label'] = value
            else:
                selection['program_id'] = value
        elif key == 'date':
            # removes spaces from daterange queries
            value = value.replace(' ', '')
            selection[key] = value
        elif key in ['ra', 'dec', 'sr', 'cenwlen', 'filepre']:
            # Check the formatting of RA, Dec, search radius values. Keep them in same format as given though.

            # Eliminate any whitespace
            value = value.replace(' ', '')

            # Should do more format verification here?
            # but don't try and interpret it here.

            # Put into selection dictionary
            selection[key] = value

        elif key == 'engineering':
            if value == 'EngExclude':
                selection[key] = False
            elif value == 'EngOnly':
                selection[key] = True
            if value == 'EngInclude':
                # dummy value
                selection[key] = 'Include'
        elif key == 'science_verification':
            if value == 'SvExclude':
                selection[key] = False
            elif value == 'SvOnly':
                selection[key] = True
            if value == 'SvInclude':
                if key in list(selection.keys()):
                    selection.pop(key)
        elif key == 'site_monitoring':
            if value == 'SmInclude':
                selection[key] = True
            elif value == 'SmExclude':
                selection[key] = False

        elif key == 'focal_plane_mask':
            if value == 'custom':
                if 'custom_mask' in list(formdata.keys()):
                    selection[key] = formdata['custom_mask'].value
            else:
                selection[key] = value
        elif key == 'custom_mask':
            # Ignore - done in focal_plane_mask
            pass
        elif key == 'col_selection':
            selection['cols'] = formdata_to_compressed(value)
        else:
            # This covers the generic case where the formdata key is also
            # the selection key, and the form value is the selection value
            selection[key] = value


def nameresolver(resolver, target):
    """
    A name resolver proxy. Pass it the resolver and object name
    """

    resp = get_context().resp
    resp.content_type = 'application/json'

    urls = {
        'simbad': 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/S?',
        'ned': 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/N?',
        'vizier': 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/V?'
    }

    try:
        url = urls[resolver] + target

        with contextlib.closing(urllib.request.urlopen(url)) as urlfd:
            xml = urllib.request.urlopen(url).read()
            doc = minidom.parseString(xml)
            info = doc.getElementsByTagName("INFO")
            if info and ('nothing found' in info[0].childNodes[0].nodeValue.lower()):
                msg = {'success': False, 'message': 'Object not found' }
            else:
                ra = float(doc.getElementsByTagName('jradeg')[0].childNodes[0].wholeText)
                dec = float(doc.getElementsByTagName('jdedeg')[0].childNodes[0].wholeText)
                msg = {'success': True, 'ra': ra, 'dec': dec}
    except KeyError:
        resp.status = Return.HTTP_NOT_ACCEPTABLE
        return
    except ExpatError:
        msg = {'success': False, 'message': "Got corrupted information from the name resolver"}
    except IndexError:
        msg = {'success': False, 'message': "The name resolver returned information in an unknown format"}
    except Exception as e:
        try:
            message = e.strerror.strerror
        except AttributeError:
            message = str(e)
        msg = {'success': False, 'message': message}
    resp.append_json(msg)


# DATA FOR THE TEMPLATES

dropdown_options = {
    "engdata_options":
        [("EngExclude", "Exclude"),
         ("EngInclude", "Include"),
         ("EngOnly", "Find Only")],
    "mode_options":
        [("imaging", "Imaging"),
         ("spectroscopy", "Spectroscopy"),
         ("LS", "Long-slit spectroscopy"),
         ("MOS", "Multi-object spectroscopy"),
         ("IFS", "Integral field spectroscopy")],
    "svdata_options":
        [("SvInclude", "Include"),
         ("SvExclude", "Exclude"),
         ("SvOnly", "Find Only")],
    "sm_options":
        [("SmExclude", "Exclude"),
         ("SmInclude", "Find Only")],
    "gmos_mask_options":
        [("0.5arcsec", "0.5 arcsec"),
         ("0.75arcsec", "0.75 arcsec"),
         ("1.0arcsec", "1.0 arcsec"),
         ("1.5arcsec", "1.5 arcsec"),
         ("2.0arcsec", "2.0 arcsec"),
         ("5.0arcsec", "5.0 arcsec"),
         ("NS0.75arcsec", "NS 0.75 arcsec"),
         ("NS1.0arcsec", "NS 1.0 arcsec"),
         ("NS1.5arcsec", "NS 1.5 arcsec"),
         ("NS2.0arcsec", "NS 2.0 arcsec"),
         ("IFU-R", "IFU-R"),
         ("IFU-B", "IFU-B"),
         ("IFU-2", "IFU-2"),
         ("Imaging", "Imaging"),
         ("focus_array_new", "focus array new"),
         ("custom", "Custom")],
    "gnirs_mask_options":
        [("0.10arcsec", "0.10 arcsec"),
         ("0.15arcsec", "0.15 arcsec"),
         ("0.20arcsec", "0.20 arcsec"),
         ("0.30arcsec", "0.30 arcsec"),
         ("0.45arcsec", "0.45 arcsec"),
         ("0.68arcsec", "0.675 arcsec"),
         ("1.0arcsec", "1.0 arcsec"),
         ("0.10arcsecXD", "0.10 arcsec XD"),
         ("0.15arcsecXD", "0.15 arcsec XD"),
         ("0.20arcsecXD", "0.20 arcsec XD"),
         ("0.30arcsecXD", "0.30 arcsec XD"),
         ("0.45arcsecXD", "0.45 arcsec XD"),
         ("0.68arcsecXD", "0.675 arcsec XD"),
         ("1.0arcsecXD", "1.0 arcsec XD"),
         ("Acq", "acquisition"),
         ("AcqXD", "acquisition XD"),
         ("SmPinholes", "pinhole 0.1"),
         ("LgPinholes", "pinhole 0.3"),
         ("SmPinholesXD", "pinhole 0.1 XD"),
         ("LgPinholesXD", "pinhole 0.3 XD"),
         ("pupilview", "pupil viewer"),
         ("IFU", "IFU")],
    "niri_mask_options":
        [("f6-2pix", "f6-2pix"),
         ("f6-2pixBl", "f6-2pix Blue"),
         ("f6-4pix", "f6-4pix"),
         ("f6-4pixBl", "f6-4pix Blue"),
         ("f6-6pix", "f6-6pix"),
         ("f6-6pixBl", "f6-6pix Blue"),
         ("f32-6pix", "f32-6pix"),
         ("f32-9pix", "f32-9pix"),
         ("f6-cam", "f6-cam"),
         ("f14-cam", "f14-cam"),
         ("f32-cam", "f32-cam"),
         ("pinhole", "pinhole")],
    "nifs_mask_options":
        [("Blocked", "Blocked"),
         ("Ronchi_Screen", "Ronchi Screen"),
         ("KG3_ND_Filter", "KG3 ND Filter"),
         ("KG5_ND_Filter", "KG5 ND Filter"),
         ("0.1_Hole", "0.1 Hole"),
         ("0.2_Occ_Disc", "0.2 Occ Disc"),
         ("0.5_Occ_Disc", "0.5 Occ Disc"),
         ("3.0_Mask", "3.0 Mask")],
    "michelle_mask_options":
        [("1_pixels", "1 pixel"),
         ("2_pixels", "2 pixels"),
         ("3_pixels", "3 pixels"),
         ("4_pixels", "4 pixels"),
         ("6_pixels", "6 pixels"),
         ("8_pixels", "8 pixels"),
         ("16_pixels", "16 pixels")],
    "trecs_mask_options":
        [("0.21", '0.21&quot;'),
         ("0.26", '0.26&quot;'),
         ("0.31", '0.31&quot;'),
         ("0.35", '0.35&quot;'),
         ("0.65", '0.65&quot;'),
         ("0.70", '0.70&quot;'),
         ("1.30", '1.30&quot;')],
    "f2_mask_options":
        [("1pix-slit", "1pix-slit"),
         ("2pix-slit", "2pix-slit"),
         ("3pix-slit", "3pix-slit"),
         ("4pix-slit", "4pix-slit"),
         ("6pix-slit", "6pix-slit"),
         ("8pix-slit", "8pix-slit")],
    "gmos_gain_options":
        [("low", "Low"),
         ("high", "High")],
    "gmos_speed_options":
        [("fast", "Fast"),
         ("slow", "Slow")],
    "gnirs_depth_options":
        [("Deep", "Deep"),
         ("Shallow", "Shallow")],
    "gnirs_readmode_options":
        [("Very_Bright_Objects", "Very Bright Objects"),
         ("Bright_Objects", "Bright Objects"),
         ("Faint_Objects", "Faint Objects"),
         ("Very_Faint_Objects", "Very Faint Objects")],
    "nifs_readmode_options":
        [("Bright_Object", "Bright Object"),
         ("Medium_Object", "Medium Object"),
         ("Faint_Object", "Faint Object")],
    "nas_options":
        [("NodAndShuffle", "Nod &amp; Shuffle"),
         ("Classic", "Classic")],
    "niri_readmode_options":
        [("High_Background", "High Background"),
         ("Medium_Background", "Medium Background"),
         ("Low_Background", "Low Background")],
    "gmos_filter_options":
        [("u", "u'"),
         ("g", "g'"),
         ("r", "r'"),
         ("i", "i'"),
         ("z", "z'"),
         ("Y", "Y"),
         ("Z", "Z"),
         ("OIII", "OIII"),
         ("OIIIC", "OIIIC"),
         ('OVI', 'OVI'),
         ('OVIC', 'OVIC'),
         ("Ha", "Ha"),
         ("HaC", "HaC"),
         ("HeII", "HeII"),
         ("HeIIC", "HeIIC"),
         ("SII", "SII"),
         ("CaT", "CaT"),
         ("Lya395", "Lya395"),
         ("ri", "ri"),
         ("open", "Open")],
    "pre_image_options":
        [("preimage", "Pre-image"),],
    "gnirs_filter_options":
        [("XD", "XD"),
         ("H2", "H2"),
         ("X", "X"),
         ("J", "J"),
         ("H", "H"),
         ("K", "K"),
         ("L", "L"),
         ("M", "M"),
         ("PAH", "PAH"),
         ("YPHOT", "YPHOT"),
         ("JPHOT", "JPHOT"),
         ("KPHOT", "KPHOT"),
         ("X_(order_6)", "X_(order_6)"),
         ("J_(order_5)", "J_(order_5)"),
         ("H_(order_4)", "H_(order_4)"),
         ("K_(order_3)", "K_(order_3)"),
         ("L_(order_2)", "L_(order_2)"),
         ("M_(order_1)", "M_(order_1)")],
    "niri_filter_options":
        [("Y", "Y"),
         ("J", "J"),
         ("H", "H"),
         ("K", "K"),
         ("K(prime)", "K'"),
         ("M(prime)", "M'"),
         ("L(prime)", "L'"),
         ("K(short)", "K (short)"),
         ("Br(alpha)", "Br-&alpha;"),
         ("Br(alpha)Con", "Br-&alpha; con"),
         ("Br(gamma)", "Br-&gamma;"),
         ("H2_1-0_S1", "H2 1-0 S1"),
         ("H2_2-1_S1", "H2 2-1 S1"),
         ("HeI", "He I"),
         ("HeI(2p2s)", "He I (2p2s)"),
         ("FeII", "Fe II"),
         ("CH4(short)", "CH4 (short)"),
         ("CH4(long)", "CH4 (long)"),
         ("CH4ice(2275)", "CH4 ice (2.275)"),
         ("CO 2-0 (bh)", "CO 2-0 (bh)"),
         ("hydrocarb", "Hydrocarbon"),
         ("H2Oice", "H2O ice (3.050)"),
         ("H2Oice(2045)", "H2O ice (2.045)"),
         ("Jcon(1065)", "J con (1.065)"),
         ("Jcon(112)", "J con (1.12)"),
         ("Jcon(121)", "J con (1.21)"),
         ("H-con(157)", "H con (1.57)"),
         ("Kcon(209)", "K con (2.09)")],
    "michelle_filter_options":
        [("F112B21", "F112B21"),
         ("F116B9", "F116B9"),
         ("F125B9", "F125B9"),
         ("F86B2", "F86B2"),
         ("I103B10", "I103B10"),
         ("I107B4", "I107B4"),
         ("I112B21", "I112B21"),
         ("I116B9", "I116B9"),
         ("I125B9", "I125B9"),
         ("I128B2", "I128B2"),
         ("I185B9", "I185B9"),
         ("I79B10", "I79B10"),
         ("I86B2", "I86B2"),
         ("I88B10", "I88B10"),
         ("I97B10", "I97B10"),
         ("IP116B9", "IP116B9"),
         ("IP125B9", "IP125B9"),
         ("IP97B10", "IP97B10"),
         ("QBlock", "QBlock")],
    "trecs_filter_options":
        [("ArIII-9.0um", "ArIII 9.0&micro;m"),
         ("N", "N"),
         ("Nprime", "N'"),
         ("NeII-12.8um", "NeII 12.8&micro;m"),
         ("NeII_ref2-13.1um", "NeII ref2 13.1&micro;m"),
         ("PAH1-8.6um", "PAH1 8.6&micro;m"),
         ("PAH2-11.3um", "PAH2 11.3&micro;m"),
         ("Qa-18.3um", "Qa 18.3&micro;m"),
         ("Qb-24.5um", "Qb 24.5&micro;m"),
         ("Qw-20.8um", "Qw 20.8&micro;m"),
         ("Si1-7.9um", "Si1 7.9&micro;m"),
         ("Si2-8.8um", "Si2 8.8&micro;m"),
         ("Si3-9.7um", "Si4 9.7&micro;m"),
         ("Si2-10.4um", "Si2 10.4&micro;m"),
         ("Si5-11.7um", "Si5 11.7&micro;m"),
         ("Si6-12.3um", "Si6 12.3&micro;m"),
         ("SIV-10.5um", "SIV 10.5&micro;m")],
    "f2_filter_options":
        [("Y", "Y"),
         ("J", "J"),
         ("H", "H"),
         ("Ks", "Ks"),
         ("K-long", "K-long"),
         ('K-red', 'K-red'),
         ('K-blue', 'K-blue'),
         ("JH", "JH"),
         ("HK", "HK"),
         ("J-lo", "J low")],
    "igrins_filter_options":
        [("H", "H"),
         ("K", "K")],
    "nici_filter_options":
        [("Ks+H", "Ks+H"),
         ("CH4-H4L+CH4-H4S", "CH4-H4L+CH4-H4S"),
         ("CH4-H4L+H", "CH4-H4L+H"),
         ("CH4-H1L+CH4-H1S", "CH4-H1L+CH4-H1S"),
         ("Kcont+CH4-H1S", "Kcont+CH4-H1S"),
         ("CH4-H4S+CH4-H4L", "CH4-H4S+CH4-H4L"),
         ("CH4-H4L", "CH4-H4L"),
         ("CH4-H4S", "CH4-H4S"),
         ("Ks+CH4-H4S", "Ks+CH4-H4S"),
         ("Lprime+CH4-H1S", "Lprime+CH4-H1S"),
         ("CH4-K5L+CH4-K5S", "CH4-K5L+CH4-K5S"),
         ("CH4-H1S+CH4-H1L", "CH4-H1S+CH4-H1L"),
         ("CH4-H1L+Br-gamma", "CH4-H1L+Br-gamma"),
         ("CH4-H4L+J", "CH4-H4L+J")],
    "gsaoi_filter_options":
        [("Z", "Z"),
         ("J", "J"),
         ("H", "H"),
         ("K", "K"),
         ("Kprime", "K(prime)"),
         ("Kshort", "K(short)"),
         ("Jcont", "J-continuum"),
         ("Hcont", "H-continuum"),
         ("CH4short", "CH4(short)"),
         ("CH4long", "CH4(long)"),
         ("Kcntshrt", "K(short) continuum"),
         ("Kcntlong", "K(long) continuum"),
         ("HeI1083", "He I 1.083"),
         ("PaG", "H I P&gamma;"),
         ("PaB", "H I P&beta;"),
         ("FeII", "[FeII] 1.644"),
         ("H2O", "H2O"),
         ("HeI-2p2s", "He I (2p2s)"),
         ("H2(1-0)", "H2 1-0 S(1)"),
         ("BrG", "H I Br&gamma;"),
         ("H2(2-1)", "H2 2-1 S(1)"),
         ("CO2360", "CO &Delta;v=2")],
    "gpi_filter_options":
        [("Y", "Y"),
         ("J", "J"),
         ("H", "H"),
         ("K1", "K1"),
         ("K2", "K2")],
    "gpi_disp_options":
        [("DISP_PRISM", "PRISM"),
         ("DISP_WOLLASTON", "WOLLASTON")],
    "gpi_pupil_options":
        [("APOD_NRM", "NRM"),
         ("APOD_CLEAR", "Clear"),
         ("APOD_CLEARGP", "ClearGp"),
         ("APOD_Y", "Y"),
         ("APOD_J", "J"),
         ("APOD_H", "H"),
         ("APOD_K1", "K1"),
         ("APOD_K2", "K2")],
    "gpi_mask_options":
        [("FPM_SCIENCE", "Science"),
         ("FPM_OPEN", "Open"),
         ("FPM_Y", "Y"),
         ("FPM_J", "J"),
         ("FPM_H", "H"),
         ("FPM_K1", "K1")],
    "gmos_disp_options":
        [("B600", "B600"),
         ("R400", "R400"),
         ("R831", "R831"),
         ("B1200", "B1200"),
         ("R150", "R150"),
         ("R600", "R600"),
         ("MIRROR", "MIRROR")],
    "gnirs_disp_options":
        [("10_mm", "10 l/mm"),
         ("32_mm", "32 l/mm"),
         ("111_mm", "111 l/mm"),
         ("10lXD", "10 l/mm XD"),
         ("32lXD", "32 l/mm XD"),
         ("111lXD", "111 l/mm XD")],
    "niri_disp_options":
        [("Hgrism", "H-grism"),
         ("Jgrism", "J-grism"),
         ("Kgrism", "K-grism"),
         ("Lgrism", "L-grism"),
         ("Mgrism", "M-grism")],
    "nifs_disp_options":
        [("Z", "Z"),
         ("J", "J"),
         ("H", "H"),
         ("K", "K"),
         ("K_Short", "K short"),
         ("K_Long", "K long"),
         ("Mirror", "Mirror")],
    "michelle_disp_options":
        [("LowN", "Low N"),
         ("MedN1", "Med N1"),
         ("MedN2", "Med N2"),
         ("LowQ", "Low Q"),
         ("Echelle", "Echelle")],
    "trecs_disp_options":
        [("LowRes-10", "Low-Res 10"),
         ("LowRes-20", "Low-Res 20")],
    "f2_disp_options":
        [("R3K", "R3K"),
         ("JH", "JH"),
         ("HK", "HK")],
    "reduction_options":
        [("RAW", "Raw Only"),
         ("PREPARED", "Reduced (not Cals)"),
         ("PROCESSED_BIAS", "Processed Biases Only"),
         ("PROCESSED_FLAT", "Processed Flats Only"),
         ("PROCESSED_FRINGE", "Processed Fringe Frames Only")],
    "qa_options":
        [("NotFail", "Not Fail"),
         ("AnyQA", "Any"),
         ("Pass", "Pass"),
         ("Lucky", "Pass or Undefined"),
         ("Win", "Pass or Usable"),
         ("Usable", "Usable"),
         ("UndefinedQA", "Undefined"),
         ("Fail", "Fail")],
    "inst_options":
        [("GMOS", "GMOS-N or GMOS-S"),
         ("GMOS-N", "GMOS-N"),
         ("GMOS-S", "GMOS-S"),
         ("GNIRS", "GNIRS"),
         ("GRACES", "GRACES"),
         ("NIRI", "NIRI"),
         ("NIFS", "NIFS"),
         ("GSAOI", "GSAOI"),
         ("F2", "F2"),
         ("GPI", "GPI"),
         ("NICI", "NICI"),
         ("IGRINS", "IGRINS"),
         ("michelle", "Michelle"),
         ("TReCS", "T-ReCS"),
         ("bHROS", "bHROS"),
         ("hrwfs", "HRWFS / AcqCam"),
         ("OSCIR", "OSCIR"),
         ("FLAMINGOS", "FLAMINGOS"),
         ("Hokupaa+QUIRC", "Hokupaa+QUIRC"),
         ("PHOENIX", "PHOENIX"),
         ("TEXES", "TEXES"),
         ("ABU", "ABU"),
         ("CIRPASS", "CIRPASS")],
    "obs_cls_options":
        [("science", "science"),
         ("acq", "acq"),
         ("progCal", "progCal"),
         ("dayCal", "dayCal"),
         ("partnerCal", "partnerCal"),
         ("acqCal", "acqCal")],
    "obs_typ_options":
        [("OBJECT", "OBJECT"),
         ("BIAS", "BIAS"),
         ("DARK", "DARK"),
         ("FLAT", "FLAT"),
         ("ARC", "ARC"),
         ("PINHOLE", "PINHOLE"),
         ("RONCHI", "RONCHI"),
         ("CAL", "CAL"),
         ("FRINGE", "FRINGE"),
         ("MASK", "MOS MASK")],
    "ao_options":
        [("NOTAO", "Not AO"),
         ("AO", "AO"),
         ("NGS", "NGS"),
         ("LGS", "LGS")],
    "bin_options":
        [("1x1", "1x1"),
         ("1x2", "1x2"),
         ("1x4", "1x4"),
         ("2x1", "2x1"),
         ("2x2", "2x2"),
         ("2x4", "2x4"),
         ("4x1", "4x1"),
         ("4x2", "4x2"),
         ("4x4", "4x4")],
    "gmos_droi_options":
        [("Full Frame", "Full Frame"),
         ("Central Spectrum", "Central Spectrum"),
         ("Central Stamp", "Central Stamp")],
    "niri_droi_options":
        [("Full Frame", "Full Frame"),
         ("Central768", "Central 768"),
         ("Central512", "Central 512"),
         ("Central256", "Central 256")],
    "gnirs_cam_options":
        [("GnirsShort", '0.15 &quot;/pix'),
         ("GnirsLong", '0.05 &quot;/pix')],
    "niri_cam_options":
        [("f6", 'f/6 (0.12 &quot;/pix)'),
         ("f13.9", 'f/14 (0.05 &quot;/pix)'),
         ("f32", 'f/32 (0.02 &quot;/pix)')]
    }
