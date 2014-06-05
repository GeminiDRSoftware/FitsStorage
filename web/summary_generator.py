"""
This module contains the web summary generator class.
"""

from collections import OrderedDict
from cgi import escape

from orm.header import Header
from gemini_metadata_utils import GeminiDataLabel, percentilestring


class SummaryGenerator():
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
    
    def __init__(self, type, links=True, uri=None):
        """
        Constructor function for the SummaryGenerator Object.
        Arguments: type = a string saying the summary type
                   links = a bool saying whether to include html links in output
        """
        # Load the column definitions, in order
        self.init_cols()
        # Set the want flags
        self.set_type(type)
        self.links = links
        self.uri = uri

    def set_type(self, type):
        """
        Sets the columns to include in the summary, based on pre-defined
        summary types.
        Valid types are: summary, ssummary, lsummary, diskfiles
        """
        sum_type_defs = {
            'summary' : ['filename', 'data_label', 'ut_datetime', 'instrument', 'observation_class', 'observation_type', 'object', 'waveband', 'exposure_time', 'airmass', 'local_time', 'qa_state', 'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg'],
            'lsummary' : ['filename', 'data_label', 'ut_datetime', 'instrument', 'observation_class', 'observation_type', 'object', 'waveband', 'exposure_time', 'airmass', 'local_time', 'filter_name', 'fpmask', 'detector_roi', 'detector_binnin', 'detector_config', 'qa_state', 'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg'],
            'ssummary' : ['filename', 'data_label', 'ut_datetime', 'instrument', 'observation_class', 'observation_type', 'object', 'waveband', 'qa_state', 'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg'],
            'diskfiles' : ['filename', 'data_label', 'ut_datetime', 'instrument', 'present', 'entrytime', 'lastmod', 'file_size', 'file_md5', 'gzipped', 'data_size', 'data_md5'],
            'searchresults' : ['download', 'filename', 'data_label', 'ut_datetime', 'instrument', 'observation_class', 'observation_type', 'object', 'waveband', 'exposure_time', 'qa_state']}
  
        if(type in sum_type_defs.keys()):
            want = sum_type_defs[type]
            for key in self.columns.keys():
                if(key in want):
                    self.columns[key]['want'] = True
                else:
                    self.columns[key]['want'] = False

    def init_cols(self):
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
        self.columns['gzipped'] = {
            'heading' : 'Gzipped',
            'longheading' : '',
            'sortarrows' : False,
            'want' : True,
            'header_attr' : None,
            'diskfile_attr' : 'gzipped',
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
        req.write('<TR class=tr_head>')
        for colkey in self.columns.keys():
            col = self.columns[colkey]
            if(col['want']):
                req.write('<TH>')
                if(col['longheading']):
                    req.write('<abbr title="%s">%s</abbr>' % (col['longheading'], col['heading']))
                else:
                    req.write(col['heading'])
                if(col['sortarrows']):
                    req.write('<a href="%s?orderby=%s_asc">&uarr;</a><a href="%s?orderby=%s_desc">&darr;</a>' % (req.uri, colkey, req.uri, colkey))
                req.write('</TH>\n')
        req.write('</TR>')

    def table_row(self, req, header, trclass=None):
        """
        Writes the html for a table row to the req object,
        for columns as configured, pulling data from the
        header object given. If trclass is supplied, it is passed as the
        class of the tr tag.
        """
        if(trclass):
            req.write('<TR class=%s>' % trclass)
        else:
            req.write('<TR>')
        for colkey in self.columns.keys():
            col = self.columns[colkey]
            if(col['want']):
                req.write('<TD>')
                if(col['summary_func']):
                    value = getattr(self, col['summary_func'])(header)
                elif(col['header_attr']):
                    value = getattr(header, col['header_attr'])
                else:
                    value = "Error: Not Defined in SummaryGenerator!"
                req.write(str(value))
                req.write('</TD>\n')
        req.write('</TR>')


    def filename(self, header):
        # Generate the filename column contents html

        # The html to return

        # The basic filename part, optionally as a link to the header text
        if(self.links):
            html = '<a href="/fullheader/%d">%s</a>' % (header.diskfile.id, header.diskfile.file.name)
        else:
            html = str(header.diskfile.file.name)

        # Do we have any fits verify errors to flag?
        if(header.diskfile.fverrors):
            if(self.links):
                html += ' <a href="/fitsverify/%d">-fits!</a>' % (header.diskfile.id)
            else:
                html += ' -fits!' % (header.diskfile.id)

        # Do we have metadata errors to flag? (only on non Eng data)
        if((header.engineering is False) and (not header.diskfile.wmdready)):
            if(self.links):
                html += ' <a href="/wmdreport/%d">-md!</a>' % (header.diskfile.id)
            else:
                html += ' -md!' % (header.diskfile.id)

        return html

    def datalabel(self, header):
        # Generate the diskfile html
        # We parse the data_label to create links to the project id and obs id
        if(self.links):
            dl = GeminiDataLabel(header.data_label)
            if(dl.datalabel):
                uri = self.uri
                html = '<a href="%s/%s">%s</a>-<a href="%s/%s">%s</a>-<a href="%s/%s">%s</a>' % (uri, dl.projectid, dl.projectid, uri, dl.observation_id, dl.obsnum, uri, dl.datalabel, dl.dlnum)
            else:
                html = str(header.data_label)
        else:
            html = str(header.data_label)

        return html

    def ut_datetime(self, header):
        # format withou decimal places on the seconds
        if(self.links):
            if(header.ut_datetime):
                date_print = header.ut_datetime.strftime("%Y-%m-%d")
                time_print = header.ut_datetime.strftime("%H:%M:%S")
                date_link = header.ut_datetime.strftime("%Y%m%d")
                return '<a href="%s/%s">%s</a> %s' % (self.uri, date_link, date_print, time_print)
            else:
                return "None"
        else:
            if(header.ut_datetime):
                return str(header.ut_datetime.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                return "None"


    def instrument(self, header):
        # Add the AO flags to the instrument name

        if(self.links):
            html = '<a href="%s/%s">%s</a>' % (self.uri, header.instrument, header.instrument)
        else:
            html = str(header.instrument)

        if(header.adaptive_optics):
            if(self.links):
                html += ' <a href="%s/AO">+ AO</a>' % self.uri
            else:
                html += ' + AO'

            if(header.laser_guide_star):
                if(self.links):
                    html += ' <a href="%s/LGS"> LGS</a>' % self.uri
                else:
                    html += ' LGS'
            else:
                if(self.links):
                    html += ' <a href="%s/NGS"> NGS</a>' % self.uri
                else:
                    html += ' NGS'

        return html

    def observation_class(self, header):
        # Can make it a link
        if(self.links):
            return '<a href="%s/%s">%s</a>' % (self.uri, header.observation_class, header.observation_class)
        else:
            return header.observation_class

    def observation_type(self, header):
        # Can make it a link
        if(self.links):
            return '<a href="%s/%s">%s</a>' % (self.uri, header.observation_type, header.observation_type)
        else:
            return header.observation_type

    def exposure_time(self, header):
        # All we do is format it with 2 decimal places
        try:
            return "%.2f" % header.exposure_time
        except:
            return ''

    def airmass(self, header):
        # All we do is format it with 2 decimal places
        try:
            return "%.2f" % header.airmass
        except:
            return ''

    def local_time(self, header):
        # All we do is format it without decimal places
        try:
            return header.local_time.strftime("%H:%M:%S")
        except:
            return ''


    def object(self, header):
        # nb target names sometime contain ampersand characters which should be escaped in the html.
        # Also we trim at 12 characters and abbreviate
        if(header.object is None):
            html = 'None'
        elif(len(header.object) > 12):
            html = '<abbr title="%s">%s</abbr>' % (htmlescape(header.object), htmlescape(header.object[:12]))
        else:
            html = htmlescape(header.object)

        # Now the photometric std star symbol
        if(header.phot_standard):
            if(self.links):
                html += ' <a href="/standardobs/%d">*</a>' % header.id
            else:
                html += ' *'

        # Now the target symbol
        if('AZEL_TARGET' in header.types and 'AT_ZENITH' not in header.types):
            html += ' <abbr title="Target is in AzEl co-ordinate frame">&#x2693</abbr>'
        if('AT_ZENITH' in header.types):
            html += ' <abbr title="Target is Zenith in AzEl co-ordinate frame">&#x2693&#x2191</abbr>'
        if('NON_SIDEREAL' in header.types):
            html += ' <abbr title="Target is non-sidereal">&#x2604</abbr>'

        return html

    def filter_name(self, header):
        # Just htmlescape it
        return htmlescape(header.filter_name)

    def waveband(self, header):
        # Print filter_name for imaging, disperser and cen_wlen for spec
        if(header.spectroscopy):
            return "%s : %.3f" % (htmlescape(header.disperser), header.central_wavelength)
        else:
            return htmlescape(header.filter_name)

    def download(self, header):
        return '<center><a href="/file/%s">[Download]</a></center>' % header.diskfile.file.name

def htmlescape(string):
    """
    Convenience wrapper to cgi escape, providing type protection
    """

    if(type(string) in [str, unicode]):
        return escape(string)
    else:
        return None
