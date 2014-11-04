"""
This module contains the web summary generator class.
"""

from collections import OrderedDict
from cgi import escape

from gemini_metadata_utils import GeminiDataLabel

from utils.userprogram import canhave

from fits_storage_config import using_previews

class SummaryGenerator(object):
    """
    This is the web summary generator class. You instantiate this class and
    configure what columns you want in your web summary, (and optionally
    what program_ids you have pre-release data access to), then the object
    provides methods to generate the header of the html table and generate
    each row of the html table from an ORM header object.
    For simplicity, there are also methods that configure the object to
    generate the "standard" summary table types.
    It is also possible to configure whether you want links in the output html.
    """

    # columns is a dictionary (collections.OrderedDict) of possible column descriptions.
    # The dictionary key is an arbitrary column name id for internal reference,
    # but is the same as used in the orderby keys
    # The value is a dictionary describing that column, with the following keys:
    #    heading: The column heading (short form)
    #    longheading: If present, this will be the abbr text on the column heading
    #    want: boolean to say whether you want this column in your table
    #    sortarrows: Says whether to put sort arrow links in the header
    #    header_attr: Attribute name of orm.header to get value from. Or None to use diskfile
    #    summary_func: Function name in this module to get this from, pass header

    columns = OrderedDict()
    my_progids = []
    links = True
    uri = None

    # These are "caches" of values used to figure out whether the user
    # has access to the file and thus whether to display the download things
    user = None
    user_progid_list = None

    def __init__(self, sumtype, links=True, uri=None, user=None, user_progid_list=None):
        """
        Constructor function for the SummaryGenerator Object.
        Arguments: sumtype = a string saying the summary type
                   links = a bool saying whether to include html links in output
        """
        # Load the column definitions, in order
        self.init_cols()
        # Set the want flags
        self.sumtype = sumtype
        self.set_type(sumtype)
        self.links = links
        self.uri = uri
        self.user = user
        self.user_progid_list = user_progid_list

    def set_type(self, sumtype):
        """
        Sets the columns to include in the summary, based on pre-defined
        summary types.
        Valid types are: summary, ssummary, lsummary, diskfiles
        """
        sum_type_defs = {
            'summary' : ['filename', 'data_label', 'ut_datetime', 'instrument', 'observation_class', 'observation_type',
                            'object', 'waveband', 'exposure_time', 'airmass', 'local_time', 'qa_state',
                            'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg'],
            'lsummary' : ['filename', 'data_label', 'ut_datetime', 'instrument', 'observation_class', 'observation_type',
                            'object', 'waveband', 'exposure_time', 'airmass', 'local_time', 'filter_name', 'fpmask',
                            'detector_roi', 'detector_binnin', 'detector_config', 'qa_state',
                            'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg'],
            'ssummary' : ['filename', 'data_label', 'ut_datetime', 'instrument', 'observation_class', 'observation_type',
                            'object', 'waveband', 'qa_state', 'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg'],
            'diskfiles' : ['filename', 'data_label', 'ut_datetime', 'instrument', 'present', 'entrytime', 'lastmod',
                            'file_size', 'file_md5', 'compressed', 'data_size', 'data_md5'],
            'searchresults' : ['download', 'filename', 'data_label', 'ut_datetime', 'instrument', 'observation_class',
                            'observation_type', 'object', 'waveband', 'exposure_time', 'qa_state'],
            'associated_cals': ['download', 'filename', 'data_label', 'ut_datetime', 'instrument', 'observation_class',
                            'observation_type', 'object', 'waveband', 'exposure_time', 'qa_state']}

        if sumtype in sum_type_defs.keys():
            want = sum_type_defs[sumtype]
            for key in self.columns.keys():
                if key in want:
                    self.columns[key]['want'] = True
                else:
                    self.columns[key]['want'] = False

    def init_cols(self):
        """
        Initializes the columns dictionary with default settings
        """
        self.columns['download'] = {
            'heading' : 'Download',
            'longheading' : None,
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'download'
            }
        self.columns['filename'] = {
            'heading' : 'Filename',
            'longheading' : None,
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'filename'
            }
        self.columns['data_label'] = {
            'heading' : 'Data Label',
            'longheading' : None,
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'datalabel'
            }
        self.columns['ut_datetime'] = {
            'heading' : 'UT Date Time',
            'longheading' : None,
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'ut_datetime'
            }
        self.columns['instrument'] = {
            'heading' : 'Inst',
            'longheading' : 'Instrument',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'instrument'
            }
        self.columns['observation_class'] = {
            'heading' : 'Class',
            'longheading' : 'Obs Class',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'observation_class'
            }
        self.columns['observation_type'] = {
            'heading' : 'Type',
            'longheading' : 'Obs Type',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'observation_type'
            }
        self.columns['object'] = {
            'heading' : 'Object',
            'longheading' : 'Target Object Name',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'object'
            }
        self.columns['waveband'] = {
            'heading' : 'WaveBand',
            'longheading' : 'Imaging Filter or Spectroscopy Disperser and Wavelength',
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'waveband'
            }
        self.columns['exposure_time'] = {
            'heading' : 'ExpT',
            'longheading' : 'Exposure Time',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'exposure_time'
            }
        self.columns['airmass'] = {
            'heading' : 'AM',
            'longheading' : 'AirMass',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'airmass'
            }
        self.columns['local_time'] = {
            'heading' : 'LclTime',
            'longheading' : 'Local Time',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'local_time'
            }
        self.columns['filter_name'] = {
            'heading' : 'Filter',
            'longheading' : 'Filter Name',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : None,
            'summary_func' : 'filter_name'
            }
        self.columns['disperser'] = {
            'heading' : 'Disperser',
            'longheading' : 'Disperser: Central Wavelength',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : 'disperser',
            'summary_func' : None
            }
        self.columns['fpmask'] = {
            'heading' : 'FP Mask',
            'longheading' : 'Focal Plane Mask',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : 'focal_plane_mask',
            'summary_func' : None
            }
        self.columns['detector_roi'] = {
            'heading' : 'ROI',
            'longheading' : 'Detector ROI',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : 'detector_roi_setting',
            'summary_func' : None
            }
        self.columns['detector_binning'] = {
            'heading' : 'Binning',
            'longheading' : 'Detector Binning',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : 'detector_binning',
            'summary_func' : None
            }
        self.columns['detector_config'] = {
            'heading' : 'DetConf',
            'longheading' : 'Detector Configuration',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : 'detector_config',
            'summary_func' : None
            }
        self.columns['qa_state'] = {
            'heading' : 'QA',
            'longheading' : 'QA State',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : 'qa_state',
            'summary_func' : None
            }
        self.columns['raw_iq'] = {
            'heading' : 'IQ',
            'longheading' : 'Raw IQ',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : 'raw_iq',
            'summary_func' : None
            }
        self.columns['raw_cc'] = {
            'heading' : 'CC',
            'longheading' : 'Raw CC',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : 'raw_cc',
            'summary_func' : None
            }
        self.columns['raw_wv'] = {
            'heading' : 'WV',
            'longheading' : 'Raw WV',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : 'raw_wv',
            'summary_func' : None
            }
        self.columns['raw_bg'] = {
            'heading' : 'BG',
            'longheading' : 'Raw BG',
            'sortarrows' : True,
            'want' : True,
            'header_attr' : 'raw_bg',
            'summary_func' : None
            }
        self.columns['present'] = {
            'heading' : 'Present',
            'longheading' : '',
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'diskfile_attr' : 'present',
            'summary_func' : None
            }
        self.columns['entrytime'] = {
            'heading' : 'Entry',
            'longheading' : '',
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'diskfile_attr' : 'entrytime',
            'summary_func' : None
            }
        self.columns['lastmod'] = {
            'heading' : 'LastMod',
            'longheading' : '',
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'diskfile_attr' : 'lastmod',
            'summary_func' : None
            }
        self.columns['file_size'] = {
            'heading' : 'File Size',
            'longheading' : '',
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'diskfile_attr' : 'file_size',
            'summary_func' : None
            }
        self.columns['file_md5'] = {
            'heading' : 'File MD5',
            'longheading' : '',
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'diskfile_attr' : 'file_md5',
            'summary_func' : None
            }
        self.columns['compressed'] = {
            'heading' : 'Compressed',
            'longheading' : '',
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'diskfile_attr' : 'compressed',
            'summary_func' : None
            }
        self.columns['data_size'] = {
            'heading' : 'Data Size',
            'longheading' : '',
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'diskfile_attr' : 'data_size',
            'summary_func' : None
            }
        self.columns['data_md5'] = {
            'heading' : 'Data MD5',
            'longheading' : '',
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'diskfile_attr' : 'data_md5',
            'summary_func' : None
            }

    def table_header(self, req):
        """
        Writes the html table header row to the req object,
        for columns as configured
        """
        # Turns out to be better performance to string concatenate
        # than to call req.write() many times, so we build an html
        # string and req.write it at the end

        html = '<TR class=tr_head>'
        for colkey in self.columns.keys():
            col = self.columns[colkey]
            if col['want']:
                html += '<TH>'
                if col['longheading']:
                    html += '<abbr title="%s">%s</abbr>' % (col['longheading'], col['heading'])
                else:
                    html += col['heading']
                if self.links and col['sortarrows']:
                    html += '<a href="%s?orderby=%s_asc">&uarr;</a><a href="%s?orderby=%s_desc">&darr;</a>' % (self.uri, colkey, self.uri, colkey)
                html += '</TH>'
        html += '</TR>\n'
        req.write(html)

    def table_row(self, req, header, trclass=None):
        """
        Writes the html for a table row to the req object,
        for columns as configured, pulling data from the
        header object given. If trclass is supplied, it is passed as the
        class of the tr tag.
        """
        # Turns out to be better performance to string concatenate
        # than to call req.write() many times, so we build an html
        # string and req.write it at the end

        if trclass:
            html = '<TR class=%s>' % trclass
        else:
            html = '<TR>'
        for colkey in self.columns.keys():
            col = self.columns[colkey]
            if col['want']:
                html += '<TD>'
                if col['summary_func']:
                    value = getattr(self, col['summary_func'])(header)
                elif col['header_attr']:
                    value = getattr(header, col['header_attr'])
                elif col['diskfile_attr']:
                    value = getattr(header.diskfile, col['diskfile_attr'])
                else:
                    value = "Error: Not Defined in SummaryGenerator!"
                html += str(value)
                html += '</TD>'
        html += '</TR>\n'
        req.write(html)


    def filename(self, header):
        """
        Generates the filename column html
        """
        # Generate the filename column contents html

        # The html to return

        # The basic filename part, optionally as a link to the header text
        if self.links:
            html = '<a href="/fullheader/%d" target="_blank">%s</a>' % (header.diskfile.id, header.diskfile.file.name)
        else:
            html = str(header.diskfile.file.name)

        # Do we have any fits verify errors to flag?
        if header.diskfile.fverrors:
            if self.links:
                html += ' <a href="/fitsverify/%d" target="_blank">-fits!</a>' % (header.diskfile.id)
            else:
                html += ' -fits!' % (header.diskfile.id)

        # Do we have metadata errors to flag? (only on non Eng data)
        if (header.engineering is False) and (not header.diskfile.wmdready):
            if self.links:
                html += ' <a href="/wmdreport/%d" target="_blank">-md!</a>' % (header.diskfile.id)
            else:
                html += ' -md!' % (header.diskfile.id)

        return html

    def datalabel(self, header):
        """
        Generates the datalabel column html
        """
        # Generate the diskfile html
        # We parse the data_label to create links to the project id and obs id
        if self.links:
            dl = GeminiDataLabel(header.data_label)
            if dl.datalabel:
                uri = self.uri
                html = '<a href="%s/%s">%s</a>-<a href="%s/%s">%s</a>-<a href="%s/%s">%s</a>' % (uri, dl.projectid, dl.projectid, uri, dl.observation_id, dl.obsnum, uri, dl.datalabel, dl.dlnum)
            else:
                html = str(header.data_label)
        else:
            html = str(header.data_label)

        return html

    def ut_datetime(self, header):
        """
        Generates the UT datetime column html
        """
        # format withou decimal places on the seconds
        if self.links and header.ut_datetime is not None:
            if header.ut_datetime:
                date_print = header.ut_datetime.strftime("%Y-%m-%d")
                time_print = header.ut_datetime.strftime("%H:%M:%S")
                date_link = header.ut_datetime.strftime("%Y%m%d")
                return '<a href="%s/%s">%s</a> %s' % (self.uri, date_link, date_print, time_print)
            else:
                return "None"
        else:
            if header.ut_datetime:
                return str(header.ut_datetime.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                return "None"


    def instrument(self, header):
        """
        Generates the instrument column html
        """
        # Add the AO flags to the instrument name
        if self.links:
            html = '<a href="%s/%s">%s</a>' % (self.uri, header.instrument, header.instrument)
            if header.adaptive_optics:
                html += ' <a href="%s/AO">+ AO</a>' % self.uri
                if header.laser_guide_star:
                    html += ' <a href="%s/LGS"> LGS</a>' % self.uri
                else:
                    html += ' <a href="%s/NGS"> NGS</a>' % self.uri
        else:
            html = str(header.instrument)
            if header.adaptive_optics:
                html += ' + AO'
                if header.laser_guide_star:
                    html += ' LGS'
                else:
                    html += ' NGS'
        return html

    def observation_class(self, header):
        """
        Generates the observation_class column html
        """
        # Can make it a link
        if self.links and header.observation_class is not None:
            return '<a href="%s/%s">%s</a>' % (self.uri, header.observation_class, header.observation_class)
        else:
            return header.observation_class

    def observation_type(self, header):
        """
        Generates the observation_type column html
        """
        # Can make it a link
        if self.links and header.observation_type is not None:
            return '<a href="%s/%s">%s</a>' % (self.uri, header.observation_type, header.observation_type)
        else:
            return header.observation_type

    def exposure_time(self, header):
        """
        Generates the exposure time column html
        """
        # All we do is format it with 2 decimal places
        try:
            return "%.2f" % header.exposure_time
        except:
            return ''

    def airmass(self, header):
        """
        Generates the airmass column html
        """
        # All we do is format it with 2 decimal places
        try:
            return "%.2f" % header.airmass
        except:
            return ''

    def local_time(self, header):
        """
        Generates the local_time column html
        """
        # All we do is format it without decimal places
        try:
            return header.local_time.strftime("%H:%M:%S")
        except:
            return ''


    def object(self, header):
        """
        Generates the object name column html
        """
        # nb target names sometime contain ampersand characters which should be escaped in the html.
        # Also we trim at 12 characters and abbreviate
        if header.object is None:
            basehtml = 'None'
        elif len(header.object) > 12:
            basehtml = '<abbr title="%s">%s</abbr>' % (htmlescape(header.object), htmlescape(header.object[:12]))
        else:
            basehtml = htmlescape(header.object)

        # Now the photometric std star symbol
        phothtml = ''
        if header.phot_standard:
            if self.links:
                phothtml = '<a href="/standardobs/%d">*</a>' % header.id
            else:
                phothtml = '*'

        # Now the target symbol
        symhtml = ''
        if 'AZEL_TARGET' in header.types and 'AT_ZENITH' not in header.types:
            symhtml = '<abbr title="Target is in AzEl co-ordinate frame">&#x2693;</abbr>'
        if 'AT_ZENITH' in header.types:
            symhtml = '<abbr title="Target is Zenith in AzEl co-ordinate frame">&#x2693;&#x2191;</abbr>'
        if 'NON_SIDEREAL' in header.types:
            symhtml = '<abbr title="Target is non-sidereal">&#x2604;</abbr>'

        return '%s %s %s' % (basehtml, phothtml, symhtml)

    def filter_name(self, header):
        """
        Generates the filter name column html
        """
        # Just htmlescape it
        return htmlescape(header.filter_name)

    def waveband(self, header):
        """
        Generates the waveband column html
        """
        # Print filter_name for imaging, disperser and cen_wlen for spec
        if header.spectroscopy:
            try:
                html = "%s : %.3f" % (htmlescape(header.disperser), header.central_wavelength)
            except:
                html = "None"
            return html
        else:
            return htmlescape(header.filter_name)

    def download(self, header):
        """
        Generates the download column html
        """
        # Determine if this user has access to this file
        can = canhave(None, self.user, header, False, self.user_progid_list)
        if can:
            html = '<div class="center">'

            # Preview link
            if using_previews:
                html += '<a href="/preview/%s">[P] </a>' % header.diskfile.file.name

            # Download link
            html += '<a href="/file/%s">[D]</a>' % header.diskfile.file.name

            # Download select button
            if self.sumtype in ['searchresults', 'associated_cals']:
                html += " <input type='checkbox' name='files' value='%s'>" % header.diskfile.file.name

            html += '</div>'

            return html
        else:
            return '<div class="center"><abbr title="This appears to be proprietary data to which you do not have access. It becomes public on %s">N/A</abbr></div>' % header.release

def htmlescape(string):
    """
    Convenience wrapper to cgi escape, providing type protection
    """

    if type(string) in [str, unicode]:
        return escape(string)
    else:
        return None

