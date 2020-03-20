"""
Utilities to deal with ODB data
"""

from xml.dom.minidom import parseString
from ..gemini_metadata_utils import GeminiProgram


def extract_data(node, replace=True):
    ret = node.childNodes[0].data
    if replace:
        # Sometimes people use ;s for separators in the odb email fields...
        return ret.replace(';', ',')
    return ret

def extract_element_by_tag_name(root, tag_name, default_val='', replace=True):
    try:
        return extract_data(root.getElementsByTagName(tag_name)[0], replace)
    except IndexError:
        return default_val

def get_programs(xdoc):
    for pg in xdoc.getElementsByTagName("program"):
        try:
            yield Program(pg)
        except NoInfoError:
            # ok, no investigators
            pass

class NoInfoError(Exception):
    pass

class Program(object):
    def __init__(self, program_node):
        self.root = program_node

    def get_investigators(self):
        """
        Return
        ------
        tuple: (<str>, <str>); investigator names, piEmail. By definitiion, the
            *first* name in the investigator names string is the PI. 

        E.g., return

        ("auth_1, auth_2, auth_3", "auth_1@goo.edu")

        """
        investigatorNames = []
        investigator_sections = self.root.getElementsByTagName('investigators')
        if all(len(iname.childNodes) == 0 for iname in investigator_sections):
            gp = GeminiProgram(self.get_reference()) # reference is 'program id'
            if not gp.is_env and not gp.is_sv:
                raise NoInfoError("There are no investigators listed for {}".format(self.get_reference()))
            else:
                # eng and sv programs can have no investigators, setting defaults for returns
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
        <list>: [ {}, {} , ... ], a list of dictionaries containing the datalabel
            and comments associated with that datalabel.

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
                    comments = [extract_data(record) for record in dset.getElementsByTagName('record')]
                    comment_string = ", ".join(c for c in comments)
                    logcomments.append({"label": did, "comment": comment_string})
        return logcomments

    def get_ngo_email(self):
        return extract_element_by_tag_name(self.root, 'ngoEmail')

    def get_reference(self):
        return extract_element_by_tag_name(self.root, 'reference')

    def get_title(self):
        return extract_element_by_tag_name(self.root, 'title')

    def get_contact(self):
        return extract_element_by_tag_name(self.root, 'contactScientistEmail')

    def get_abstract(self):
        return extract_element_by_tag_name(self.root, 'abstrakt', default_val="No abstract")

    def get_notify(self):
        return extract_element_by_tag_name(self.root, "notifyPi", default_val="No", replace=False)

def build_odbdata(programs):
    semester_data = []
    for program in programs:
        try:
            odb_data = {}
            odb_data['reference'] = program.get_reference()
            odb_data['title'] = program.get_title()
            odb_data['contactScientistEmail'] = program.get_contact()
            odb_data['abstrakt'] = program.get_abstract()
            odb_data['investigatorNames'], odb_data['piEmail'] = program.get_investigators()
            odb_data['observations'] = program.get_obslog_comms()
            semester_data.append(odb_data)
        except NoInfoError as exception:
            print(exception.message)
    return semester_data
