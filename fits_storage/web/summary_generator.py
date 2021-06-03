"""
This module contains the web summary generator class.
"""

from collections import OrderedDict, namedtuple
from html import escape

from ..gemini_metadata_utils import GeminiDataLabel, degtora, degtodec

from ..utils.userprogram import canhave_header, canhave_coords

from ..fits_storage_config import using_previews

# The following dictionary maps column key names as used in the summary template
# with a pair of values (column name, compressed name). The column name refers
# to the internal key used by the SummaryGenerator to access a column definition.
# The compressed name is a character (typically a letter, but not restricted to it)
# used in links to the searchform to persist the columns that should be displayed.

search_col_mapping = {
#   col_key    (column_name, compressed_name)
    'col_cls': ('observation_class', 'C'),
    'col_typ': ('observation_type', 'T'),
    'col_obj': ('object', 'O'),
    'col_wvb': ('waveband', 'W'),
    'col_exp': ('exposure_time', 'E'),
    'col_air': ('airmass', 'A'),
    'col_flt': ('filter_name', 'F'),
    'col_fpm': ('focal_plane_mask', 'M'),
    'col_bin': ('detector_binning', 'B'),
    'col_cwl': ('central_wavelength', 'L'),
    'col_dis': ('disperser', 'D'),
    'col_ra' : ('ra', 'r'),
    'col_dec': ('dec', 'd'),
    'col_qas': ('qa_state', 'Q'),
    'col_riq': ('raw_iq', 'i'),
    'col_rcc': ('raw_cc', 'c'),
    'col_rwv': ('raw_wv', 'w'),
    'col_rbg': ('raw_bg', 'b'),
}
# Note that the order these come out in is actually the same as the order
# of the columns buttons in the searchform.

rev_map_comp = dict((v[1], k) for (k, v) in list(search_col_mapping.items()))

default_search_cols = [ 'col_cls', 'col_typ', 'col_obj', 'col_wvb', 'col_exp', 'col_qas' ]

def formdata_to_compressed(selected):
    return ''.join([search_col_mapping[k][1] for k in selected])

def selection_to_form_indices(selection):
    return tuple(rev_map_comp[x] for x in selection['cols'])

def selection_to_column_names(selection):
    try:
        cols = selection_to_form_indices(selection)
    except KeyError:
        # Default case. 'cols' was not in selections
        cols = default_search_cols
    return tuple(search_col_mapping[x][0] for x in cols)

sum_type_defs = {
    'summary' : ['filename', 'procmode', 'data_label', 'ut_datetime', 'instrument', 'observation_class', 'observation_type',
                    'object', 'waveband', 'exposure_time', 'airmass', 'local_time', 'qa_state',
                    'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg'],
    'lsummary' : ['filename', 'procmode', 'data_label', 'ut_datetime', 'instrument', 'observation_class', 'observation_type',
                    'object', 'waveband', 'exposure_time', 'airmass', 'local_time', 'filter_name', 'focal_plane_mask',
                    'detector_roi', 'detector_binning', 'detector_gain_setting', 'detector_readmode_setting', 'qa_state',
                    'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg'],
    'ssummary' : ['filename', 'data_label', 'ut_datetime', 'instrument', 'observation_class', 'observation_type',
                    'object', 'waveband', 'qa_state', 'raw_iq', 'raw_cc', 'raw_wv', 'raw_bg'],
    'diskfiles' : ['filename', 'data_label', 'ut_datetime', 'instrument', 'present', 'entrytime', 'lastmod',
                    'file_size', 'file_md5', 'compressed', 'data_size', 'data_md5'],
    'searchresults' : ['download', 'filename', 'procmode', 'data_label', 'ut_datetime', 'instrument', 'observation_class',
                    'observation_type', 'object', 'waveband', 'exposure_time', 'qa_state'],
    'customsearch'  : ['download', 'filename', 'procmode', 'data_label', 'ut_datetime', 'instrument'],
    'associated_cals': ['download', 'filename', 'procmode', 'data_label', 'ut_datetime', 'instrument', 'observation_class',
                    'observation_type', 'object', 'waveband', 'exposure_time', 'qa_state']
    }

NO_LINKS        = 0x00
SORT_ARROWS     = 0x01
FILENAME_LINKS  = 0x02
ALL_LINKS       = 0xFF

# Named tuple to hold the column definitions. Before this we used dictionaries, but it's kind of messy and
# verbose to use def['blah'] instead of def.blah
ColDef = namedtuple('ColDef', "heading longheading sortarrows want header_attr diskfile_attr summary_func")
# There are less default values than fields in the namedtuple. This is to force heading having a value
# This means that the first element in the defaults corresponds to 'longheading'
ColDef.__new__.__defaults__ = (None, True, False, None, None, None)

class ColWrapper(object):
    """
    This class wraps column data to present it in a useful way to the template that will render it.

    The column wrapper gets extra attributes added to it. See SummaryGenerator.table_row for details.
    """
    def __init__(self, summary, key, coldef):
        self._arrows  = (summary.links & SORT_ARROWS) != 0
        self.key      = key
        self._coldef  = coldef

    def __getattr__(self, attr):
        return getattr(self._coldef, attr)

    @property
    def sortarrow(self):
        "Boolean. Should this column present sort arrows?"
        return self._arrows and self._coldef.sortarrows

    def __str__(self):
        if hasattr(self, 'content'):
            return "<ColWrapper '{}' {}>".format(self.key, str(self.content))

        return "<ColWrapper '{}'>".format(self.key)

class Row(object):
    """
    Simple object to group column data related to the same target. The main use for it
    is to carry the "can be downloaded?" information
    """
    def __init__(self):
        self.can_download = False
        self.columns = []

    def add(self, coltext):
        self.columns.append(coltext)

class SummaryGenerator(object):
    """
    This is the web summary generator class. You instantiate this class and
    configure what columns you want in your web summary, (and optionally
    what program_ids you have pre-release data access to), then the object
    provides methods to generate the header and row data, from which HTML (or
    other) can be creted.

    For simplicity, there are also methods that configure the object to
    generate the "standard" summary table types.

    It is also possible to configure whether you want links in the output.

    See the comments on the `table_row` method to learn the kind of information
    that is returned for each column.
    """

    def __init__(self, sumtype, links=ALL_LINKS, uri=None, user=None, user_progid_list=None, user_obsid_list=None,
                 user_file_list=None, additional_columns=()):
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
        self.wanted = sum_type_defs[sumtype] + list(additional_columns)
        self.links = links
        self.uri = uri
        self.my_progids = []

    # These are "caches" of values used to figure out whether the user
    # has access to the file and thus whether to display the download things
        self.user = user
        self.user_progid_list = user_progid_list
        self.user_obsid_list = user_obsid_list
        self.user_file_list = user_file_list


    def init_cols(self):
        """
        Initializes the columns dictionary with default settings
        """
        self.columns = {
            'download':    ColDef(heading      = 'Download',
                                  sortarrows   = False,
                                  summary_func = 'download'),
            'procmode':     ColDef(heading      = 'Qual',
                                  sortarrows   = False,
                                  summary_func = 'procmode'),
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
                                  longheading  = 'Imaging Filter or Spectroscopy Disperser and Wavelength',
                                  sortarrows   = False,
                                  summary_func = 'waveband'),
            'exposure_time':
                           ColDef(heading      = 'ExpT',
                                  longheading  = 'Exposure Time',
                                  summary_func = 'exposure_time'),
            'airmass':     ColDef(heading      = 'AM',
                                  longheading  = 'AirMass',
                                  summary_func = 'airmass'),
            'ra':          ColDef(heading      = 'RA',
                                  longheading  = 'Right Ascension',
                                  summary_func = 'ra'),
            'dec':         ColDef(heading      = 'Dec',
                                  longheading  = 'Declination',
                                  summary_func = 'dec'),
            'local_time':  ColDef(heading      = 'LclTime',
                                  longheading  = 'Local Time',
                                  summary_func = 'local_time'),
            'filter_name': ColDef(heading      = 'Filter',
                                  longheading  = 'Filter Name',
                                  header_attr  = 'filter_name'),
            'disperser':   ColDef(heading      = 'Disperser',
                                  longheading  = 'Disperser',
                                  header_attr  = 'disperser'),
            'central_wavelength': 
                           ColDef(heading      = 'Wavelength',
                                  longheading  = 'Central Wavelength',
                                  header_attr  = 'central_wavelength'),
            'focal_plane_mask': ColDef(heading = 'FP Mask',
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
            'detector_gain_setting':
                           ColDef(heading      = 'DetGain',
                                  longheading  = 'Detector Gain',
                                  header_attr  = 'detector_gain_setting'),
            'detector_readmode_setting':
                           ColDef(heading      = 'DetMode',
                                  longheading  = 'Detector Read Mode',
                                  header_attr  = 'detector_readmode_setting'),
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
            'entrytime':   ColDef(heading      = 'Entry Time',
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

    def table_row(self, header, diskfile, file, comment=None, preview=None):
        """
        Returns a row object for c for columns as configured, pulling data from the
        header object given.
        """

        row = Row()

        row.uri = self.uri
        row.procmode = header.procmode
        if diskfile.provenance:
            row.has_provenance = True
        else:
            row.has_provenance = False
        for colkey, col in ((x, self.columns[x]) for x in self.wanted):
            c = ColWrapper(self, colkey, col)

            # Most of our columns consists on just some text to be displayed.
            # We set said text to the column wrapper using the `text` attribute
            #
            # Some other columns carry more information than just the text
            # (links to be added, downloadable, etc). This is encoded in a
            # dictionary, which i added to the column wrapper using the
            # `content` attribute.
            #
            # The dictionary can be one of two kinds:
            #
            #  - If the data/metadata is proprietary and should not be displayed,
            #    a dictionary consisting on 'prop_message' and 'release' keys is
            #    returned. The prop_message is a hint on what should be displayed
            #    (eg. on an HTML page). 'release' is the release date of the
            #    proprietary data.
            #
            #  - Otherwise, an arbitrary dictionary of data is returned. There's
            #    no standard definition for this, and it is defined by the needs
            #    of the rendering end.
            #
            # It's up to the content generation device (eg. a template) to figure
            # out which one is to be used, and to render the information accordingly.
            #
            # The following code figures out where to extra the information from,
            # and sets the appropriate attribute.

            c.text = None
            c.content = None
            if col.summary_func:
                preview = None
                if diskfile.previews:
                    preview = diskfile.previews[0]
                value = getattr(self, col.summary_func)(header=header,
                                                        diskfile=diskfile,
                                                        file=file,
                                                        preview=preview,
                                                        comment=comment)
                if colkey == 'download' and 'downloadable' in value:
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

    def filename(self, header, diskfile, file, **kw):
        """
        Generates the filename column data
        """
        # Determine if this user can have the link to the header
        if canhave_coords(None, self.user, header, user_progid_list=self.user_progid_list,
                          user_obsid_list=self.user_obsid_list, user_file_list=self.user_file_list):
            return dict(
                links = self.links != NO_LINKS,
                name  = file.name,
                df_id = diskfile.id,
                # Booleans indicating if error information is present
                fverr = diskfile.fverrors != 0,
                mderr = (header.engineering is False) and (not diskfile.mdready)
                )
        else:
            return dict(prop_message=file.name, release=header.release)

    def procmode(self, header, **kw):
        """
        Get the quality of the file (science quality, quick look...)

        :param header:
        :param diskfile:
        :param file:
        :param kw:
        :return:
        """
        return header.procmode

    def datalabel(self, header, comment, **kw):
        """
        Generates the datalabel column data
        """
        # We parse the data_label to create links to the project id and obs id
        return dict(
            links     = self.links == ALL_LINKS,
            datalabel = str(header.data_label),
            dl        = GeminiDataLabel(header.data_label),
            comment   = comment.comment if comment is not None else None,
            display_prog  = False if header.calibration_program else True
            )

    def ut_datetime(self, header, **kw):
        """
        Generates the UT datetime column data
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

    def instrument(self, header, **kw):
        """
        Generates the instrument column data
        """
        # Add the AO flags to the instrument name
        return dict(
            links = self.links == ALL_LINKS,
            inst  = header.instrument,
            ao    = header.adaptive_optics,
            lg    = header.laser_guide_star
            )

    def observation_class(self, header, **kw):
        """
        Generates the observation_class column data
        """
        return dict(
            links = (self.links == ALL_LINKS) and header.observation_class is not None,
            text  = header.observation_class
        )

    def observation_type(self, header, **kw):
        """
        Generates the observation_type column data
        """
        return dict(
            links = (self.links == ALL_LINKS) and header.observation_type is not None,
            text  = header.observation_type
        )

    def exposure_time(self, header, **kw):
        """
        Generates the exposure time column data
        """
        # All we do is format it with 2 decimal places
        try:
            return "%.2f" % header.exposure_time
        except (TypeError, AttributeError):
            return ''

    def airmass(self, header, **kw):
        """
        Generates the airmass column data
        """
        # Determine if this user can see this info
        if canhave_coords(None, self.user, header, user_progid_list=self.user_progid_list,
                          user_obsid_list=self.user_obsid_list, user_file_list=self.user_file_list):
            # All we do is format it with 2 decimal places
            try:
                return "%.2f" % header.airmass
            except (TypeError, AttributeError):
                return ''
        else:
            return dict(prop_message='N/A', release=header.release)

    def ra(self, header, **kw):
        """
        Generates the RA
        """
        # Determine if this user can see this info
        if canhave_coords(None, self.user, header, user_progid_list=self.user_progid_list,
                          user_obsid_list=self.user_obsid_list, user_file_list=self.user_file_list):
            # Sexadeimal format
            try:
                return degtora(float(header.ra))
            except (TypeError, AttributeError):
                return ''
        else:
            return dict(prop_message='N/A', release=header.release)

    def dec(self, header, **kw):
        """
        Generates the Dec
        """
        # Determine if this user can see this info
        if canhave_coords(None, self.user, header, user_progid_list=self.user_progid_list,
                          user_obsid_list=self.user_obsid_list, user_file_list=self.user_file_list):
            # Sexadeimal format
            try:
                return degtodec(float(header.dec))
            except (TypeError, AttributeError):
                return ''
        else:
            return dict(prop_message='N/A', release=header.release)

    def local_time(self, header, **kw):
        """
        Generates the local_time column data
        """
        # All we do is format it without decimal places
        try:
            return header.local_time.strftime("%H:%M:%S")
        except (TypeError, AttributeError):
            return ''


    def object(self, header, **kw):
        """
        Generates the object name column data
        """
        # Determine if this user can see this info
        if canhave_coords(None, self.user, header, user_progid_list=self.user_progid_list,
                          user_obsid_list=self.user_obsid_list, user_file_list=self.user_file_list):
            # nb target names sometime contain ampersand characters which should be escaped in html.
            # Be careful with those.
            name = str(header.object)
            ret = dict(
                links = self.links == ALL_LINKS,
                id    = header.id,
                name  = name,
                )

            # Some rendering (eg. HTML) may want to abbreviate the content if longer than 12 characters
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

    def waveband(self, header, **kw):
        """
        Generates the waveband column data
        """
        # filter_name for imaging, disperser and cen_wlen for spec
        if header.spectroscopy and header.instrument != 'GPI':
            try:
                return "{} : {:.3f}".format(header.disperser, header.central_wavelength)
            except:
                return "None"
        else:
            return header.filter_name

    def download(self, header, diskfile, file, preview, **kw):
        """
        Generates the download column data
        """
        # Determine if this user has access to this file
        if canhave_header(None, self.user, header, user_progid_list=self.user_progid_list,
                          user_obsid_list=self.user_obsid_list):
            ret = dict(name=file.name)
            # Preview link
            if using_previews and preview is not None:
                ret['prev'] = True

            # Download select button
            if self.sumtype in ['searchresults', 'customsearch', 'associated_cals']:
                ret['down_sel'] = True

            ret['downloadable'] = True

            return ret
        else:
            return dict(
                prop_message='Proprietary',
                release=header.release,
                centered=True
            )
