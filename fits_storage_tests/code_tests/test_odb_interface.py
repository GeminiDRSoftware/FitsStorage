import os.path
import json

from fits_storage_tests.code_tests.helpers import make_empty_testing_db_env

import fits_storage_tests

from fits_storage.server.odb_program_interface import get_odb_prog_dicts
from fits_storage.server.odb_data_handlers import update_notifications, \
    update_programs

from fits_storage.server.orm.notification import Notification
from fits_storage.server.orm.program import Program
from fits_storage.server.orm.obslog_comment import ObslogComment

from fits_storage.db import sessionfactory
from fits_storage.logger_dummy import DummyLogger


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
    #     json.dump(result, g)

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

    ols = p0['obslog_comments']
    assert len(ols) == 2
    ol = p0['obslog_comments'][0]
    assert ol['label'] == 'GN-2009B-Q-123-45-67'
    assert ol['comment'] == 'Full of stars'

    with open(json_file, 'r') as g:
        jsontext = g.read()
    assert jsontext == json.dumps(result)


def test_update_notifications(tmp_path):
    make_empty_testing_db_env(tmp_path)
    session = sessionfactory()
    logger = DummyLogger()

    sample_file = os.path.join(fits_storage_tests.__path__[0],
                               'odbbrowser_sample.xml')
    with open(sample_file, 'r') as f:
        xml = f.read()

    programs = get_odb_prog_dicts(None, None, xml_inject=xml)
    update_notifications(session, programs, logger)

    # Check the notifications table directly.
    l = 'Auto - GN-2009B-Q-123'
    n = session.query(Notification).filter(Notification.label == l).first()

    assert n is not None
    assert n.selection == 'GN-2009B-Q-123/science'
    assert n.piemail == 'mm@disney.edu'
    assert n.csemail == 'csperson@gemini.edu, anothercs@gemini.edu'
    assert n.ngoemail == 'ngoperson@gemini.edu'

    # Find the program in the list and mess with it
    for p in programs:
        if p['id'] == 'GN-2009B-Q-123':
            p['piEmail'] = 'foobar@disney.edu'

    # Update the notifications
    update_notifications(session, programs, logger)

    # Verify we got the new email
    n = session.query(Notification).filter(Notification.label == l).first()
    assert n.selection == 'GN-2009B-Q-123/science'
    assert n.piemail == 'foobar@disney.edu'

    # Find the program in the list and mess with it again
    for p in programs:
        if p['id'] == 'GN-2009B-Q-123':
            p['notify'] = False

    # Update the notifications
    update_notifications(session, programs, logger)

    # Verify no notification now.
    n = session.query(Notification).filter(Notification.label == l).first()
    assert n is None


def test_update_programs(tmp_path):
    make_empty_testing_db_env(tmp_path)
    session = sessionfactory()
    logger = DummyLogger()

    sample_file = os.path.join(fits_storage_tests.__path__[0],
                               'odbbrowser_sample.xml')
    with open(sample_file, 'r') as f:
        xml = f.read()

    programs = get_odb_prog_dicts(None, None, xml_inject=xml)
    update_programs(session, programs, logger)

    # Check the programs table directly.
    l = 'GN-2009B-Q-123'
    p = session.query(Program).filter(Program.program_id == l).first()

    assert p is not None
    assert p.program_id == 'GN-2009B-Q-123'
    assert p.title == 'Star Formation Rates'
    assert p.abstract == 'Understanding galaxies is hard. We did some things ' \
                         'and we need to do more things.'
    assert p.pi_coi_names == 'Mickey Mouse, Daffy Duck'

    # Find the program in the list and mess with it
    for p in programs:
        if p['id'] == 'GN-2009B-Q-123':
            p['investigator_names'] = 'My program Now'

    # Update the programs
    update_programs(session, programs, logger)

    # Verify we got the new investigator names
    p = session.query(Program).filter(Program.program_id == l).first()
    assert p.program_id == 'GN-2009B-Q-123'
    assert p.pi_coi_names == 'My program Now'

    # Check the obslogComments table directly
    l = session.query(ObslogComment).\
        filter(ObslogComment.data_label == 'GN-2009B-Q-123-45-67').first()
    assert l.data_label == 'GN-2009B-Q-123-45-67'
    assert l.comment == 'Full of stars'
