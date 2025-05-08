import re

# We start off here by constructing regular expressions to match program ID
# strings, and then observation ID strings and datalabel strings.
#
# Because there are (now) a large variety of program ID strings in circulation,
# we do this step-by-step, with regexes to match each variety that are then
# combined into one regex with |s.
# Where applicable the groups should label the following groups:
# ec: ENG|CAL|COM|MON, typ: Q|C|etc, sem: semester eg 2012A, date: yyyymmdd.
# Note, match group names have to be unique, even if they're in mutually
# exclusive parts of the regex, so we define e.g. ec1, ec2, ec3 group names, and
# we combine these before we test against them.
#
# Note, we don't put ^ and $ to match the start and end of the string as we use
# eg the progid regexes to build regesed for obsid too. Instead of these, we use
# the re.fullmatch function throughout.
#
# G[N|S]-[CAL|ENG]yyyymmdd eg GN-CAL20120123. There is a modest attempt at
# date enforcement in that years must be 20xx, months [01]x and days [0123]x.
pid_caleng_orig = r'G[NS]-(?P<ec1>CAL|ENG)(?P<date1>20\d\d[01]\d[0123]\d)'

# There are a few archaic ones that are just CALyyyymmdd or ENGyyyymmdd
pid_caleng_archaic = r'(?P<ec2>CAL|ENG)(?P<date2>20\d\d[01]\d[0123]\d)'

# At some point things like GN-2020A-CAL-191 became a thing...
pid_caleng_another = r'G[NS]-(?P<sem1>20\d\d[AB])-(?P<ec3>CAL|ENG)-\d+'

# And the "New" 2024 GPP format is: G-YYYYS-TYP-<inst>-NN
# Where TYP can be [CAL|ENG|COM|MON] - COMmissinong and instrument MONitoring.
pid_caleng_new = r'G-(?P<sem2>20\d\d[AB])-(?P<ec4>CAL|ENG|COM|MON)-[\w+-]+-\d+'

pid_caleng = r'%s|%s|%s|%s' % (pid_caleng_new, pid_caleng_orig,
                              pid_caleng_another, pid_caleng_archaic)
pid_caleng_cre = re.compile(pid_caleng)

# And now science program ids
# Original format was GN-2011A-Q-123
pid_sci_orig = r'G[NS]-(?P<sem3>20\d\d[AB])-(?P<typ1>Q|C|SV|QS|DD|LP|FT|DS)-\d+'

# New 2024 GPP format is: G-YYYYS-NNNN-T
# Where T is a single letter from [CDFLQSVP]
pid_sci_new = r'G-(?P<sem4>20\d\d[AB])-\d+-(?P<typ2>[CDFLQSVP])'

# This should match any valid program science program ID
pid_sci = "%s|%s" % (pid_sci_orig, pid_sci_new)
pid_sci_cre = re.compile(pid_sci)

# Finally, this should match any valid program ID
pid = "%s|%s" % (pid_sci, pid_caleng)
pid_cre = re.compile(pid)

# This matches an observation id with the project id and obsnum as groups
obsid = r"(?P<progid>%s)-(?P<obsid>\d+)" % pid
obsid_cre = re.compile(obsid)

# This matches a data-label with an optional extension
# ie program_id-obsnum-dlnum[-ext], with groups progid, obsid, dlid, extn.
# Note the extn has to start with a letter to avoid confusion with the various
# program ID and thus datalabel formats.
dl = r'(?P<progid>%s)-(?P<obsid>\d+)-(?P<dlid>\d+)' \
     r'(?:-(?P<extn>[A-Za-z]\w*))?' % pid
dl_cre = re.compile(dl)


class GeminiDataLabel:
    """
    GeminiDataLabel class. Construct an instance from a datalabel string, you
    can then call various methods to evaluate properties of the data label.
    """

    def __init__(self, dl: str):
        """
        Construct a GeminiDataLabel from the given datalabel string.

        This will parse the passed datalabel and fill in the various fields
        with values inferred from the datalabel.

        dl: str
            datalabel to use
        """
        # Clean up datalabel if it has space padding
        if not isinstance(dl, str):
            raise ValueError('Value passed to GeminiDataLabel must be a str')
        dl = dl.strip()

        self.datalabel = dl                # datalabel as a string
        self.program_id = None             # program id portion
        self._program = None               # GeminiProgram instance
        self.observation_id = None         # observation id portion
        self.obsnum = None                 # observation number
        self.dlnum = None                  # datalabel number
        self.extension = None              # extension number, if any
        self.datalabel_noextension = None  # datalabel without extension number
        self.valid = False                 # True if datalabel is valid format

        if self.datalabel:
            self.parse()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.datalabel)

    def parse(self):
        """
        Infer the other fields for this GeminiDataLabel based on the
        text datalabel.
        """
        dlm = dl_cre.fullmatch(self.datalabel)
        if dlm:
            self.program_id = dlm.group('progid')
            self.obsnum = dlm.group('obsid')
            self.dlnum = dlm.group('dlid')
            self.extension = dlm.group('extn')
            self.observation_id = '%s-%s' % (self.program_id, self.obsnum)
            self.datalabel_noextension = '%s-%s-%s' % (self.program_id,
                                                       self.obsnum, self.dlnum)
            self.valid = True
        else:
            # Match failed - Null the datalabel field
            self.datalabel = ''
            self.valid = False

    @property
    def program(self):
        # Lazy load the GeminiProgram instance
        if self._program is None:
            if self.program_id is None:
                return None
            self._program = GeminiProgram(self.program_id)
        return self._program


class GeminiObservation:
    """
    The GeminiObservation class parses an observation ID

    Simply instantiate the class with an observation id string
    then reference the following data members:

    * observation_id: The observation ID provided. If the class cannot
                     make sense of the string passed in, this field will
                     be empty
    * program: A GeminiProgram object for the project this is part of
    * obsnum: The observation numer within the project

    Parameters
    ----------
    observation_id : str
        Observation ID from which to parse the information
    """

    def __init__(self, observation_id):
        # Clean up value if it has space padding
        if isinstance(observation_id, str):
            observation_id = observation_id.strip()

        self._program = None

        if observation_id:
            match = re.fullmatch(obsid_cre, observation_id)
            if match:
                self.observation_id = observation_id
                self.program_id = match.group('progid')
                self.obsnum = match.group('obsid')
                self.valid = True
            else:
                self.observation_id = ''
                self.program_id = None
                self.obsnum = ''
                self.valid = False
        else:
            self.observation_id = ''
            self.valid = False

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.observation_id)
    @property
    def program(self):
        # Lazy load the GeminiProgram instance
        if self._program is None:
            if self.program_id is None:
                return None
            self._program = GeminiProgram(self.program_id)
        return self._program

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
    * is_com: Boolean indicating if this is a Commissioning program
    * is_mon: Boolean indicating if this is an instrument Monitoring program
    * is_q: Boolean indicating if this is a Queue program
    * is_c: Boolean indicating if this is a Classical program
    * is_sv: Boolean indicating this is an SV (Science Verification) program
    * is_qs: Boolean indicating if this is an QS (Quick Start) program
    * is_dd: Boolean indicating if this is an DD (Directors Discretion) program
    * is_lp: Boolean indicating if this is an LP (Large Program) program
    * is_ft: Boolean indicating if this is an FT (Fast Turnaround) program
    * is_ds: Boolean indicating if this is an DS (Demo Science) program
    * is_pw: Boolean indicating if this is a P (Poor Weather) program

    This could be easily expanded to extract semester, hemisphere, program
    number etc. if required.

    Parameters
    ----------
    program_id : str
        Gemini ProgramID to parse
    """
    def __init__(self, program_id):
        if not isinstance(program_id, str):
            raise ValueError("Must initialize a GeminiProgram with a str")

        # clean up any spaces
        program_id = program_id.strip()

        # Initialize all the is_ flags to None. These will get set to True
        # or False if they are validly assessed.
        self.is_cal = None
        self.is_eng = None
        self.is_com = None
        self.is_mon = None
        self.is_q = None
        self.is_c = None
        self.is_sv = None
        self.is_qs = None
        self.is_dd = None
        self.is_lp = None
        self.is_ft = None
        self.is_ds = None
        self.is_pw = None

        m = re.fullmatch(pid_cre, program_id)
        if m:
            self.valid = True
            self.program_id = program_id

            # We need to 'combine' the multiple group names here (see note with
            # the regex definitions about unique group names)
            ec = None
            for i in ['ec1', 'ec2', 'ec3', 'ec4']:
                j = m.groupdict().get(i)
                ec = j if j else ec
            self.is_eng = ec == 'ENG'
            self.is_cal = ec == 'CAL'
            self.is_com = ec == 'COM'
            self.is_mon = ec == 'MON'

            typ = None
            for i in ['typ1', 'typ2']:
                j = m.groupdict().get(i)
                typ = j if j else typ
            self.is_q = typ == 'Q'
            self.is_c = typ == 'C'
            self.is_sv = typ in ['V', 'SV']
            self.is_ft = typ in ['F', 'FT']
            self.is_ds = typ in ['S', 'DS']
            self.is_dd = typ in ['D', 'DD']
            self.is_ll = typ in ['L', 'LP']
            self.is_pw = typ == 'P'

            sem = None
            for i in ['sem1', 'sem2', 'sem3', 'sem4']:
                j = m.groupdict().get(i)
                sem = j if j else sem
            self.semester = sem

            date = None
            for i in ['date1', 'date2']:
                j = m.groupdict().get(i)
                date = j if j else date
            self.date = date

        else:
            # Not a valid format. Probably some kind of engineering test program
            # that someone just made up.
            self.valid = False
            self.program_id = None
            self.is_eng = True

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.program_id)