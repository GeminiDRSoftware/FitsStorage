"""
Utilities to interface with the ODB. This is for science program data, which
includes:
- program info (ie to populate the programs table)
- contact info for new data email notifications.
- obslog comments. These are ingested at the same time we ingest program info.

This code talks to the ODB "odbbrowser/programs" interface, and parses the
XML that comes back into a python data structure (list of dictionaries) that can
be used natively and also represented by a JSON document.

The data from the ODB is used by both the summit fits servers (which send the
new data notifications) and the archive (which needs the program info and
obslog comments). Currently, the ODB is network-visible to the summit fits
servers, but not to the archive server, so we use the summit fits servers to
"relay" the data to the archvie server by HTTP POSTing the JSON document
containing this data to a URL on the archive server.
"""
import requests
import http

from xml.dom.minidom import parseString

from fits_storage.gemini_metadata_utils import GeminiProgram

from fits_storage.logger import DummyLogger


def extract_data(node, replace=True):
    """
    Pulls the data out of the xml node.

    This pulls data from the first child of the XML node

    Parameters
    ----------
    node : :class:`xml.dom.Node`
        XML node to work with
    replace : bool
        True if we need to fix the data, for email addresses where we need to
        change ; separators to ,

    Returns
    -------
    str : The contents of the data in the first child, with corrections for
          email lists if requested.
    """
    ret = node.childNodes[0].data
    if replace:
        # Sometimes people use ;s for separators in the odb email fields...
        return ret.replace(';', ',')
    return ret


def extract_element_by_tag_name(root, tag_name, default_val='', replace=True):
    """
    Get the element under a root by its tag name and get the data.

    Parameters
    ----------
    root : :class:`xml.dom.Node`
        Root node to search for element
    tag_name : str
        Name of element to find
    default_val : str, optional
        Value to use in case the element is not found
    replace : bool
        If true, perform corrections as needed.  In particular, this fixed
        lists of emails to be , delimited
    """
    try:
        return extract_data(root.getElementsByTagName(tag_name)[0], replace)
    except IndexError:
        return default_val


def get_programs(xdoc):
    """
    Get the programs from the XML document.

    Parameters
    ----------
    xdoc : :class:`xml.dom.Document`
        XML document to parse

    Returns
    -------
    iterable of :class:`~OdbProgram` data
    """
    for pg in xdoc.getElementsByTagName("program"):
        try:
            yield OdbProgram(pg)
        except NoInfoError:
            # ok, no investigators
            pass


class NoInfoError(Exception):
    """
    Error to use when there is no information.
    """
    def __init__(self, message=""):
        super().__init__(self)
        self.message = message


class OdbProgram(object):
    def __init__(self, program_node):
        self.root = program_node

    def get_investigators(self):
        """
        Return
        ------
        tuple: (<str>, <str>); investigator names, piEmail. By definition, the
            *first* name in the investigator names string is the PI. 

        E.g., return

        ("auth_1, auth_2, auth_3", "auth_1@goo.edu")

        Raises
        ------
        NoInfoError
            If there were no investigators and this was not an eng, sv or cal
            program
        """
        # catch-all setting for eventual return, just in case it hits the edge
        # case with no piEmail. may refactor, but for now not keen to alter the
        # functional logic that was here already - OO / RC?
        inames = ''
        piEmail = ''
        investigatorNames = []
        investigator_sections = self.root.getElementsByTagName('investigators')
        if all(len(iname.childNodes) == 0 for iname in investigator_sections):
            gp = GeminiProgram(self.get_reference()) # reference is 'program id'
            if not gp.is_eng and not gp.is_sv and not gp.is_cal:
                raise NoInfoError("There are no investigators listed for {}"
                                  .format(self.get_reference()))
            else:
                # eng and sv programs can have no investigators, setting
                # defaults for returns
                inames = ''
                piEmail = ''

        for iname in investigator_sections:
            for n in iname.getElementsByTagName('investigator'):
                name_actual = extract_element_by_tag_name(n, 'name')
                if n.attributes.get('pi').value == 'true':
                    piEmail = extract_element_by_tag_name(n, 'email')
                    investigatorNames.insert(0, name_actual)
                else:
                    investigatorNames.append(name_actual)
            inames = ', '.join(f for f in investigatorNames)

        return inames, piEmail

    def get_obslog_comms(self):
        """
        Return
        ------
        <list>: [ {}, {} , ... ], a list of dictionaries containing the
            datalabel and comments associated with that datalabel.

        E.g.,

        [ { "label": "GN-2012A-Q-114-34-004", 
            "comment": "Not applying more offsets, ... "},
          { ... },
          ...
        ]

        """
        logcomments = []
        for obs in self.root.getElementsByTagName('observations'):
            for olog in obs.getElementsByTagName('obsLog'):
                for dset in olog.getElementsByTagName('dataset'):
                    did = extract_element_by_tag_name(dset, 'id')
                    comments = [extract_data(record) for record in
                                dset.getElementsByTagName('record')]
                    comment_string = ", ".join(c for c in comments)
                    logcomments.append({"label": did,
                                        "comment": comment_string})
        return logcomments

    def get_ngo_email(self):
        return extract_element_by_tag_name(self.root, 'ngoEmail')

    def get_reference(self):
        return extract_element_by_tag_name(self.root, 'reference')

    def get_title(self):
        return extract_element_by_tag_name(self.root, 'title')

    def get_contact(self):
        return extract_element_by_tag_name(self.root, 'contactScientistEmail')

    def get_too(self):
        return extract_element_by_tag_name(self.root, 'tooStatus')

    def get_abstract(self):
        return extract_element_by_tag_name(self.root, 'abstrakt',
                                           default_val="No abstract")

    def get_semester(self):
        return extract_element_by_tag_name(self.root, 'semester',
                                           default_val="No semester")

    def get_notify(self):
        return extract_element_by_tag_name(self.root, "notifyPi",
                                           default_val="No", replace=False)


def build_odb_progdicts(programs):
    """
    Builds a list of dictionaries describing the programs.

    If any of the programs are missing the required investigators, they
    are skipped.

    Parameters
    ----------
    programs : list of :class:`~OdbProgram`
        The programs to convert to a list of dicts

    Returns
    -------
    list of dict : Dictionaries with the basic program information
    """
    progdicts = []
    for program in programs:
        try:
            odb_data = {}
            odb_data['id'] = program.get_reference()
            odb_data['semester'] = program.get_semester()
            odb_data['title'] = program.get_title()
            odb_data['csEmail'] = program.get_contact()
            odb_data['ngoEmail'] = program.get_ngo_email()
            odb_data['too'] = program.get_too()
            odb_data['abstract'] = program.get_abstract()
            odb_data['investigator_names'], odb_data['piEmail'] = \
                program.get_investigators()
            odb_data['observations'] = program.get_obslog_comms()
            odb_data['notify'] = program.get_notify() == 'Yes'
            odb_data['obslog_comments'] = program.get_obslog_comms()
            progdicts.append(odb_data)
        except NoInfoError as exception:
            print(exception.message)
    return progdicts


"""
Notifications utils - add / update notification table entries from ODB XML
"""


def get_odb_prog_dicts(odb, semester, active=False, notifypi=None, logger=None,
                       xml_inject=None):
    """
    Fetch the ODB XML, parse it, and return a list of dictionaries containing
    the program info from the ODB.

    odb should be the hostname of the ODB server to query
    semester is a semester name (eg 2012A) or None to fetch all active programs
    active sets programActive=yes in the ODB URL
    notifypi sets programNotifyPi=true in the ODB URL
    logger is where log messages will be written.

    if xml_inject is set, the http query to the ODB will be skipped and the xml
    passed as this argument will be used as the ODB response. This argument
    is intended only to facilitate testing
    """

    if xml_inject is None:
        xml = fetch_odb_xml(odb, semester, active, notifypi, logger)
    else:
        xml = xml_inject

    if xml is None:
        logger.error("Did not get ODB xml to parse")
        return None

    # Parse the XML and build the dictionary to return.
    dom = parseString(xml)
    return build_odb_progdicts(get_programs(dom))


def fetch_odb_xml(odb, semester, active=False, notifypi=None, logger=None):
    """
    Fetch the XML from the ODB, return a list of dictionaries containing
    the program information. Write messages to the logger passed.

    odb should be the hostname of the ODB server to query
    semester is a semester name (eg 2012A) or None to fetch all active programs
    active sets programActive=yes in the ODB URL
    notifypi sets programNotifyPi=trus in the ODB URL
    logger is where log messages will be written.
    """

    if logger is None:
        logger = DummyLogger()

    # Construct the ODB URL
    url = "http://%s:8442/odbbrowser/observations" % odb
    if semester is None:
        logger.info("Fetching %s XML program info for all semesters" % odb)
        url += "?programSemester=20*"
    else:
        logger.info("Fetching %s program info for semester %s", odb, semester)
        url += "?programSemester=%s" % semester

    if active:
        url += "&programActive=yes"
    if notifypi:
        url += "&programNotifyPi=true"

    logger.debug("URL is: %s" % url)

    # Fetch the xml
    try:
        r = requests.get(url)
    except TimeoutError:
        logger.error("Timeout trying to fetch program info from ODB")
        return None
    xml = r.text
    logger.debug("Got %d bytes from server.", len(xml))
    if r.status_code == http.HTTPStatus.OK:
        logger.debug("Got HTTP OK status from ODB XML fetch")
    else:
        logger.error("Got bad http status code from ODB: %s", r.status_code)
        return None

    return xml
