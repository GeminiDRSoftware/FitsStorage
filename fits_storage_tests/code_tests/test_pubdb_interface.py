import os.path
import json

import fits_storage_tests

from fits_storage.server.publications_db_interface import get_publications


def test_pubdb_interface():
    sample_file = os.path.join(fits_storage_tests.__path__[0],
                               'publications_sample.json')
    reference_file = os.path.join(fits_storage_tests.__path__[0],
                                  'publications_reference.json')
    with open(sample_file, 'r') as f:
        text = f.read()

    result = get_publications(json_inject=text)

    # Uncomment to write new reference file
    # with open(reference_file, 'w') as g:
    #     json.dump(result, g)

    assert isinstance(result, list)
    p0 = result[0]
    assert isinstance(p0, dict)
    assert p0['author'] == 'Mouse, M., Duck, D.'

    progids = ['GN-2009B-Q-123', 'GN-2011A-Q-111']
    assert p0['program_ids'] == progids

    with open(reference_file, 'r') as g:
        jsontext = g.read()
    assert jsontext == json.dumps(result)

