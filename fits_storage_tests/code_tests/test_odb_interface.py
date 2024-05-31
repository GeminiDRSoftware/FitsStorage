import os.path
import json

import fits_storage_tests

from fits_storage.server.odb_program_interface import get_odb_prog_dicts


def test_odb_interface():
    sample_file = os.path.join(fits_storage_tests.__path__[0],
                               'odbbrowser_sample.xml')
    json_file = os.path.join(fits_storage_tests.__path__[0],
                               'odbbrowser_sample.json')
    with open(sample_file, 'r') as f:
        xml = f.read()

    result = get_odb_prog_dicts(None, None, xml_inject=xml)

    # Uncomment to write new json file
    # with open(json_file, 'w') as g:
    #    json.dump(result, g)

    assert isinstance(result, list)
    assert len(result) == 5

    p0 = result[0]
    assert isinstance(p0, dict)
    assert p0['semester'] == '2009B'
    assert p0['title'] == 'Star Formation Rates'
    assert p0['id'] == 'GN-2009B-Q-123'
    assert p0['piEmail'] == 'mm@disney.edu'
    assert p0['csEmail'] == 'csperson@gemini.edu, anothercs@gemini.edu'
    assert p0['ngoEmail'] == 'ngoperson@gemini.edu'

    with open(json_file, 'r') as g:
        jsontext = g.read()

    assert jsontext == json.dumps(result)
    