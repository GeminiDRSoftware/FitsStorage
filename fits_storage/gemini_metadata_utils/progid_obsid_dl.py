import re


# This regex matches a program id like GN-CAL20091020 with no groups
calengre_old = r'G[NS]-((?:CAL)|(?:ENG))20\d\d[01]\d[0123]\d'
calengre = r'^G[NS]?-20\d\d[ABFDLWVSX]-((?:CAL)|(?:ENG))-' \
           r'([A-Za-z0-9_]*[A-Za-z_]+[A-Za-z0-9_]*-)?\d+'

# G-YYYYT-M-BNNN  T is one of:
#  A/B - regular semester program
#  F - FT
#  D - DD
#  L - LP
#  W - PW
#  V - SV
#  S - DS
#  X - XT
# M is observing mode "(Q/C), could add P for PV"
scire = r"^(G[NS]?)-(20\d\d([A-Z]))-(Q|C|SV|QS|DD|LP|FT|DS|ENG|CAL)-(\d+)"

# This matches a program id
progre = r'(?:^%s$)|(?:^%s$)|(?:^%s$)' % (calengre, scire, calengre_old)

# This matches an observation id with the project id and obsnum as groups
obsre = r'((?:^%s)|(?:^%s)|(?:^%s))-(?P<obsid>\d*)$' % \
        (calengre, scire, calengre_old)


# The Gemini Data Label Class

# This regex matches program_id-obsum-dlnum - ie a datalabel,
# With 3 groups - program_id, obsnum, dlnum
# This also allows for an optional -blah on the end (processed biases etc.)

dlcre = re.compile(r'^(?P<progid>(?:%s)|(?:%s)|(?:%s))-(?P<obsid>\d*)-'
                   r'(?P<dlid>\d*)(?:-(?P<extn>[-\w]*))?$' %
                   (calengre, scire, calengre_old))
# dlcre = re.compile(r'^((?:%s)|(?:%s)|(?:%s))-(\d*)-(\d*)(?:-([-\w]*))?$' %\
#   (r'^G[NS]?-20\d\d[ABFDLWVSX]-((?:CAL)|(?:ENG))-(?:[A-Za-z0-9_]+-)?\d+',
#   r"^(?:G[NS]?)-(?:20\d\d([A-Z]))-(?:Q|C|SV|QS|DD|LP|FT|DS|ENG|CAL)-(?:\d+)",
#   r'G[NS]-((?:CAL)|(?:ENG))20\d\d[01]\d[0123]\d'))


class GeminiDataLabel:
    """
    Construct a GeminiDataLabel from the given datalabel string.

    This will parse the passed datalabel and fill in the various fields
    with values inferred from the datalabel.

    dl: str
        datalabel to use
    """

    datalabel = ''
    projectid = ''
    observation_id = ''
    obsnum = ''
    dlnum = ''
    extension = ''
    project = ''

    def __init__(self, dl: str):
        """
        Construct a GeminiDataLabel from the given datalabel string.

        This will parse the passed datalabel and fill in the various fields
        with values inferred from the datalabel.

        dl: str
            datalabel to use
        """
        # Clean up datalabel if it has space padding
        if dl is not None and isinstance(dl, str):
            dl = dl.strip()

        self.datalabel = dl              # datalabel as a string
        self.projectid = ''              # project id portion
        self.project = None              # GeminiProgram instance
        self.observation_id = ''         # observaiton id portion
        self.obsnum = ''                 # observation number
        self.dlnum = ''                  # datalabel number
        self.extension = ''              # extension number, if any
        self.datalabel_noextension = ''  # datalabel without extension number
        self.valid = False               # True if datalabel is valid format

        if self.datalabel:
            self.parse()

    def parse(self):
        """
        Infer the other fields for this GeminiDataLabel based on the
        text datalabel.
        """
        dlm = dlcre.match(self.datalabel)
        if dlm:
            self.projectid = dlm.group('progid')
            self.obsnum = dlm.group('obsid')
            self.dlnum = dlm.group('dlid')
            self.extension = dlm.group('extn')
            self.project = GeminiProgram(self.projectid)
            self.observation_id = '%s-%s' % (self.projectid, self.obsnum)
            self.datalabel_noextension = '%s-%s-%s' % (self.projectid,
                                                       self.obsnum, self.dlnum)
            self.valid = True
        else:
            # Match failed - Null the datalabel field
            self.datalabel = ''
            self.valid = False


class GeminiObservation:
    """
    The GeminiObservation class parses an observation ID

    Simply instantiate the class with an observation id string
    then reference the following data members:

    * observation_id: The observation ID provided. If the class cannot
                     make sense of the string passed in, this field will
                     be empty
    * project: A GeminiProgram object for the project this is part of
    * obsnum: The observation numer within the project

    Parameters
    ----------
    observation_id : str
        ObservationID from which to parse the information
    """
    observation_id = ''
    program = ''
    obsnum = ''

    def __init__(self, observation_id):
        # Clean up value if it has space padding
        if observation_id is not None and isinstance(observation_id, str):
            observation_id = observation_id.strip()

        if observation_id:
            match = re.match(obsre, observation_id)
            if match:
                self.observation_id = observation_id
                self.program = GeminiProgram(match.group(1))
                self.obsnum = match.group('obsid')
                self.valid = True
            else:
                self.observation_id = ''
                self.project = ''
                self.obsnum = ''
                self.valid = False
        else:
            self.observation_id = ''
            self.valid = False


class GeminiProgram:
    """
    The GeminiProgram class parses a Gemini Program ID and provides
    various useful information deduced from it.

    Simply instantiate the class with a program ID string, then
    reference the following data members:

    * program_id: The program ID passed in.
    * valid: Boolean indicating the program_id is a valid standard format
    * is_cal: Boolean indicating if this is a CAL program
    * is_eng: Boolean indicating if this is an ENG program
    * is_q: Boolean indicating if this is a Queue program
    * is_c: Boolean indicating if this is a Classical program
    * is_sv: Boolean indicating this is an SV (Science Verification) program
    * is_qs: Boolean indicating if this is an QS (Quick Start) program
    * is_dd: Boolean indicating if this is an DD (Directors Discretion) program
    * is_lp: Boolean indicating if this is an LP (Large Program) program
    * is_ft: Boolean indicating if this is an FT (Fast Turnaround) program
    * is_ds: Boolean indicating if this is an DS (Demo Science) program

    This could be easily expanded to extract semester, hemisphere, program
    number etc. if required.

    Parameters
    ----------
    program_id : str
        Gemini ProgramID to parse
    """
    program_id = None
    valid = None
    is_cal = False
    is_eng = False
    is_q = False
    is_c = False
    is_sv = False
    is_qs = False
    is_dd = False
    is_lp = False
    is_ft = False
    is_ds = False

    def __init__(self, program_id: str):
        # clean up any spaces
        if program_id is not None and isinstance(program_id, str):
            program_id = program_id.strip()

        self.program_id = program_id
        # Check for the CAL / ENG form
        ec_match_old = re.match(calengre_old + r'$', program_id)
        ec_match = re.match(calengre + r'$', program_id)
        sci_match = re.match(scire + r'$', program_id)
        if ec_match_old:
            # Valid eng / cal form
            self.valid = True
            self.is_eng = ec_match_old.group(1) == 'ENG'
            self.is_cal = ec_match_old.group(1) == 'CAL'
        elif ec_match:
            self.valid = True
            self.is_eng = ec_match.group(1) == 'ENG'
            self.is_cal = ec_match.group(1) == 'CAL'
        elif sci_match:
            # Valid science form
            self.valid = True
            self.is_q = sci_match.group(4) == 'Q'
            self.is_c = sci_match.group(4) == 'C'
            self.is_eng = sci_match.group(4) == 'ENG'
            self.is_cal = sci_match.group(4) == 'CAL'
            if program_id.startswith('G-'):
                self.is_sv = sci_match.group(3) == 'V'
                self.is_ft = sci_match.group(3) == 'F'
                self.is_ds = sci_match.group(3) == 'S'
            else:
                self.is_sv = sci_match.group(4) == 'SV'
                self.is_ft = sci_match.group(4) == 'FT'
                self.is_ds = sci_match.group(4) == 'DS'

            # If the program id is OLD style and program number contained
            # leading zeros, strip them out of the official program_id
            if sci_match.group(5)[0] == '0' and not program_id.startswith('G-'):
                prog_num = int(sci_match.group(5))
                self.program_id = "%s-%s-%s-%s" % (sci_match.group(1),
                                                   sci_match.group(2),
                                                   sci_match.group(4),
                                                   prog_num)

        else:
            # Not a valid format. Probably some kind of engineering test program
            # that someone just made up.
            self.valid = False
            self.is_eng = True
