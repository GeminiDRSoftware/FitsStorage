"""
This module contains the web summary generator class.
"""

from collections import OrderedDict, namedtuple
from cgi import escape

from ..gemini_metadata_utils import GeminiDataLabel

from ..utils.userprogram import canhave_header, canhave_coords

from ..fits_storage_config import using_previews

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
                    'observation_type', 'object', 'waveband', 'exposure_time', 'qa_state']
    }

NO_LINKS        = 0x00
SORT_ARROWS     = 0x01
FILENAME_LINKS  = 0x02
ALL_LINKS       = 0xFF

ColDef = namedtuple('ColDef', "heading longheading sortarrows want header_attr diskfile_attr summary_func")
# There are less default values than fields in the namedtuple. This is to force heading having a value
# This means that the first element in the defaults corresponds to 'longheading'
ColDef.__new__.__defaults__ = (None, True, False, None, None, None)

class ColWrapper(object):
    def __init__(self, summary, key, coldef):
        self._arrows  = (summary.links & SORT_ARROWS) != 0
        self.key      = key
        self._coldef  = coldef

    def __getattr__(self, attr):
        return getattr(self._coldef, attr)

    @property
    def sortarrow(self):
        return self._arrows or self._coldef.sortarrows

    def __str__(self):
        if hasattr(self, 'content'):
            return "<ColWrapper '{}' {}>".format(self.key, str(self.content))

        return "<ColWrapper '{}'>".format(self.key)

class Row(object):
    def __init__(self):
        self.can_download = False
        self.columns = []

    def add(self, coltext):
        self.columns.append(coltext)

#    def with_class(self, class_):
#        ce = 'TD' if not self.is_header else 'TH'
#        rc = (" class='" + class_ + "'") if class_ else ""
#
#        pattern = "<TR{row_class}>" + ''.join("<{col_element}>" + str(x) + "</{col_element}>" for x in self.columns) + "</TR>"
#
#        return pattern.format(row_class=rc, col_element=ce)
#
#    def __str__(self):
#        return self.with_class(self.class_)

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

    def __init__(self, sumtype, links=ALL_LINKS, uri=None, user=None, user_progid_list=None):
        """
        Constructor function for the SummaryGenerator Object.
        Arguments: sumtype = a string saying the summary type
                   links = a bool saying whether to include html links in output
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
        self.columns = OrderedDict()

        # Load the column definitions, in order
        self.init_cols()
        # Set the want flags
        self.sumtype = sumtype
        self.wanted = sum_type_defs[sumtype]
#        self.set_type(sumtype)
        self.links = links
        self.uri = uri
        self.my_progids = []

    # These are "caches" of values used to figure out whether the user
    # has access to the file and thus whether to display the download things
        self.user = user
        self.user_progid_list = user_progid_list


    def set_type(self, sumtype):
        """
        Sets the columns to include in the summary, based on pre-defined
        summary types.
        Valid types are: summary, ssummary, lsummary, diskfiles
        """

        try:
            want = sum_type_defs[sumtype]
            for key in self.columns:
                self.columns[key]['want'] = key in want
        except KeyError:
            pass

    def init_cols(self):
        """
        Initializes the columns dictionary with default settings
        """
        self.columns = {
            'download':    ColDef(heading      = 'Download',
                                  sortarrows   = False,
                                  summary_func = 'download'),
            'filename':    ColDef(heading      = 'Filename',
                                  summary_func = 'filename'),
            'data_label':  ColDef(heading      = 'Data Label',
                                  summary_func = 'datalabel'),
            'ut_datetime': ColDef(heading      = 'UT Date Time',
                                  summary_func = 'ut_datetime'),
            'instrument':  ColDef(heading      = 'Inst',
                                  longheading  = 'Instrument',
                                  summary_func = 'instrument'),
            'observation_class':
                           ColDef(heading      = 'Class',
                                  longheading  = 'Obs Class',
                                  summary_func = 'observation_class'),
            'observation_type':
                           ColDef(heading      = 'Type',
                                  longheading  = 'Obs Type',
                                  summary_func = 'observation_type'),
            'object':      ColDef(heading      = 'Object',
                                  longheading  = 'Target Object Name',
                                  summary_func = 'object'),
            'waveband':    ColDef(heading      = 'WaveBand',
                                  longheading  = 'Imaging Filter or Spectroscopy Disperser and Wavelenght',
                                  sortarrows   = False,
                                  summary_func = 'waveband'),
            'exposure_time':
                           ColDef(heading      = 'ExpT',
                                  longheading  = 'Exposure Time',
                                  summary_func = 'exposure_time'),
            'airmass':     ColDef(heading      = 'AM',
                                  longheading  = 'AirMass',
                                  summary_func = 'airmass'),
            'local_time':  ColDef(heading      = 'LclTime',
                                  longheading  = 'Local Time',
                                  summary_func = 'local_time'),
            'filter_name': ColDef(heading      = 'Filter',
                                  longheading  = 'Filter Name',
                                  header_attr  = 'filter_name'),
            'disperser':   ColDef(heading      = 'Disperser',
                                  longheading  = 'Disperser: Central Wavelength',
                                  header_attr  = 'disperser'),
            'fpmask':      ColDef(heading      = 'FP Mask',
                                  longheading  = 'Focal Plane Mask',
                                  header_attr  = 'focal_plane_mask'),
            'detector_roi':
                           ColDef(heading      = 'FP Mask',
                                  longheading  = 'Detector ROI',
                                  header_attr  = 'detector_roi_setting'),
            'detector_binning':
                           ColDef(heading      = 'Binning',
                                  longheading  = 'Detector Binning',
                                  header_attr  = 'detector_binning'),
            'detector_config':
                           ColDef(heading      = 'DetConf',
                                  longheading  = 'Detector Configuration',
                                  header_attr  = 'detector_config'),
            'qa_state':    ColDef(heading      = 'QA',
                                  longheading  = 'QA State',
                                  header_attr  = 'qa_state'),
            'raw_iq':      ColDef(heading      = 'IQ',
                                  longheading  = 'Raw IQ',
                                  header_attr  = 'raw_iq'),
            'raw_cc':      ColDef(heading      = 'CC',
                                  longheading  = 'Raw CC',
                                  header_attr  = 'raw_cc'),
            'raw_wv':      ColDef(heading      = 'WV',
                                  longheading  = 'Raw WV',
                                  header_attr  = 'raw_wv'),
            'raw_bg':      ColDef(heading      = 'BG',
                                  longheading  = 'Raw BG',
                                  header_attr  = 'raw_bg'),
            'present':     ColDef(heading      = 'Present',
                                  sortarrows   = False,
                                  diskfile_attr = 'present'),
            'entrytime':   ColDef(heading      = 'Present',
                                  sortarrows   = False,
                                  diskfile_attr = 'entrytime'),
            'lastmod':     ColDef(heading      = 'LastMod',
                                  sortarrows   = False,
                                  diskfile_attr = 'lastmod'),
            'file_size':   ColDef(heading      = 'File Size',
                                  sortarrows   = False,
                                  diskfile_attr = 'file_size'),
            'file_md5':    ColDef(heading      = 'File MD5',
                                  sortarrows   = False,
                                  diskfile_attr = 'file_md5'),
            'compressed':  ColDef(heading      = 'Compressed',
                                  sortarrows   = False,
                                  diskfile_attr = 'compressed'),
            'data_size':   ColDef(heading      = 'Data Size',
                                  sortarrows   = False,
                                  diskfile_attr = 'data_size'),
            'data_md5':    ColDef(heading      = 'Data MD5',
                                  sortarrows   = False,
                                  diskfile_attr = 'data_md5'),
        }

    def table_header(self):
        """
        Returns a header Row object for columns as configured
        """

        for colkey, col in ((x, self.columns[x]) for x in self.wanted):
            yield ColWrapper(self, colkey, col)

    def table_row(self, header, diskfile, file):
        """
        Returns a row object for c for columns as configured, pulling data from the
        header object given.
        """

        row = Row()

        row.uri = self.uri
        for colkey, col in ((x, self.columns[x]) for x in self.wanted):
            c = ColWrapper(self, colkey, col)
            if col.summary_func:
                value = getattr(self, col.summary_func)(header, diskfile, file)
                if colkey == 'download' and '[D]' in value:
                    row.can_download = True
                if isinstance(value, dict):
                    c.content = value
                else:
                    c.text    = value
            elif col.header_attr:
                c.text = getattr(header, col.header_attr)
            elif col.diskfile_attr:
                c.text = getattr(diskfile, col.diskfile_attr)
            else:
                c.text = "Error: Not Defined in SummaryGenerator!"
            row.add(c)

        return row

    def filename(self, header, diskfile, file):
        """
        Generates the filename column html
        """
        # Generate the filename column contents html

        # The html to return

        # Determine if this user can have the link to the header
        if canhave_coords(None, self.user, header, user_progid_list=self.user_progid_list):
            return dict(
                links = self.links != NO_LINKS,
                name  = file.name,
                df_id = diskfile.id,
                fverr = diskfile.fverrors != 0,
                mderr = (header.engineering is False) and (not diskfile.mdready)
                )
        else:
            return dict(prop_message=file.name, release=header.release)

    def datalabel(self, header, *args):
        """
        Generates the datalabel column html
        """
        # Generate the diskfile html
        # We parse the data_label to create links to the project id and obs id
        return dict(
            links     = self.links == ALL_LINKS,
            datalabel = str(header.data_label),
            dl        = GeminiDataLabel(header.data_label),
            )

    def ut_datetime(self, header, *args):
        """
        Generates the UT datetime column html
        """
        links = (self.links == ALL_LINKS) and header.ut_datetime is not None
        ret = dict(links = links)
        # format without decimal places on the seconds
        if header.ut_datetime is not None:
            if links:
                ret.update(dict(
                    dp = header.ut_datetime.strftime("%Y-%m-%d"),
                    tp = header.ut_datetime.strftime("%H:%M:%S"),
                    dl = header.ut_datetime.strftime("%Y%m%d")
                ))
            else:
                ret['dt'] = header.ut_datetime.strftime("%Y-%m-%d %H:%M:%S")
        else:
            ret['dt'] = 'None'

        return ret

    def instrument(self, header, *args):
        """
        Generates the instrument column html
        """
        # Add the AO flags to the instrument name
        return dict(
            links = self.links == ALL_LINKS,
            inst  = header.instrument,
            ao    = header.adaptive_optics,
            lg    = header.laser_guide_star
            )

    def observation_class(self, header, *args):
        """
        Generates the observation_class column html
        """
        return dict(
            links = (self.links == ALL_LINKS) and header.observation_class is not None,
            text  = header.observation_class
        )

    def observation_type(self, header, *args):
        """
        Generates the observation_type column html
        """
        return dict(
            links = (self.links == ALL_LINKS) and header.observation_type is not None,
            text  = header.observation_type
        )

    def exposure_time(self, header, *args):
        """
        Generates the exposure time column html
        """
        # All we do is format it with 2 decimal places
        try:
            return "%.2f" % header.exposure_time
        except (TypeError, AttributeError):
            return ''

    def airmass(self, header, *args):
        """
        Generates the airmass column html
        """
        # Determine if this user can see this info
        if canhave_coords(None, self.user, header, user_progid_list=self.user_progid_list):
            # All we do is format it with 2 decimal places
            try:
                return "%.2f" % header.airmass
            except (TypeError, AttributeError):
                return ''
        else:
            return dict(prop_message='N/A', release=header.release)

    def local_time(self, header, *args):
        """
        Generates the local_time column html
        """
        # All we do is format it without decimal places
        try:
            return header.local_time.strftime("%H:%M:%S")
        except (TypeError, AttributeError):
            return ''


    def object(self, header, *args):
        """
        Generates the object name column html
        """
        # Determine if this user can see this info
        if canhave_coords(None, self.user, header, user_progid_list=self.user_progid_list):
            # nb target names sometime contain ampersand characters which should be escaped in the html.
            # Also we trim at 12 characters and abbreviate
            name = str(header.object)
            ret = dict(
                links = self.links == ALL_LINKS,
                id    = header.id,
                name  = name,
                )
            if len(name) > 12:
                ret['abbr'] = True

            if header.phot_standard:
                ret['photstd'] = True

            if header.types is not None:
                if 'AT_ZENITH' in header.types:
                    ret['type'] = 'zen'
                elif 'AZEL_TARGET' in header.types:
                    ret['type'] = 'azeltgt'
                elif 'NON_SIDEREAL' in header.types:
                    ret['type'] = 'ns'

            return ret
        else:
            return dict(prop_message='N/A', release=header.release)

    def waveband(self, header, *args):
        """
        Generates the waveband column html
        """
        # Print filter_name for imaging, disperser and cen_wlen for spec
        if header.spectroscopy and header.instrument != 'GPI':
            try:
                return "{} : {:.3f}".format(header.disperser, header.central_wavelength)
            except:
                return "None"
        else:
            return header.filter_name

    def download(self, header, diskfile, file):
        """
        Generates the download column html
        """
        # Determine if this user has access to this file
        if canhave_header(None, self.user, header, user_progid_list=self.user_progid_list):
            html = '<div class="center">'

            ret = dict(name=file.name)
            # Preview link
            if using_previews:
                ret['prev'] = True

            # Download select button
            if self.sumtype in ['searchresults', 'associated_cals']:
                ret['down_sel'] = True

            return ret
        else:
            return dict(
                prop_message=header.release.strftime('%Y%m%d'),
                release=header.release,
                centered=True
            )
