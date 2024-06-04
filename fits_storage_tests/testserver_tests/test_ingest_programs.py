import os
import json
import requests

import fits_storage_tests
from fits_storage_tests.testserver_tests.helpers import getserver

from fits_storage.server.odb_program_interface import get_odb_prog_dicts


def test_ingest_programs():
    # Get the ODB sample data
    sample_file = os.path.join(fits_storage_tests.__path__[0],
                               'odbbrowser_sample.xml')
    with open(sample_file, 'r') as f:
        xml = f.read()

    progs = get_odb_prog_dicts(None, None, xml_inject=xml)
    text = json.dumps(progs)

    url = getserver() + "/ingest_programs"

    req = requests.post(url, data=text)

    assert req.status_code == 200

    # Verify program data actually made it
    url = getserver() + "/programinfo/GN-2009B-Q-123"
    req = requests.get(url)
    assert req.status_code == 200
    assert 'Star Formation Rates' in req.text
    assert 'Understanding galaxies is hard.' in req.text

