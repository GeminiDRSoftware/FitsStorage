from fits_storage.gemini_metadata_utils.progid_obsid_dl import *


def test_pid_caleng_regexes():
    # First, test out the individual program id variety regexes
    orig_valid = ['GN-CAL20120123', 'GS-CAL20000131', 'GN-ENG20441231']
    orig_invalid = ['G-CAL20120123', 'GX-CAL20000131', 'GN-ENG19441231']
    for i in orig_valid:
        assert re.fullmatch(pid_caleng_orig, i) is not None
    for i in orig_invalid:
        assert re.fullmatch(pid_caleng_orig, i) is None

    archaic_valid = ['CAL20031122', 'ENG20221101']
    archaic_invalid = ['CAL1901122', 'Eng20221101', 'CAL200312']
    for i in archaic_valid:
        assert re.fullmatch(pid_caleng_archaic, i) is not None
    for i in archaic_invalid:
        assert re.fullmatch(pid_caleng_archaic, i) is None

    another_valid = ['GN-2020A-CAL-191', 'GN-2020B-ENG-191']
    another_invalid = ['GN-2020A-foo-191', 'G-2020B-ENG-191']
    for i in another_valid:
        assert re.fullmatch(pid_caleng_another, i) is not None
    for i in another_invalid:
        assert re.fullmatch(pid_caleng_another, i) is None

    new_valid = ['G-2001A-CAL-GMOS_N-20', 'G-2001B-ENG-GMOS_N-20',
                 'G-2001A-COM-IGRINS2-20', 'G-2001A-MON-F_2-20',
                 'G-2001B-CAL-GMOS_N-100']
    new_invalid = ['GN-2001A-CAL-GMOS_N-20', 'G-2001B-FOO-GMOS_N-20',
                   'G-2001A-COM-IGRINS-2-20']
    for i in new_valid:
        assert re.fullmatch(pid_caleng_new, i) is not None
    for i in new_invalid:
        assert re.fullmatch(pid_caleng_new, i) is None

    # And now the combined one, to check for crosstalk...
    all_valid = orig_valid + archaic_valid + another_valid + new_valid
    all_invalid = orig_invalid + archaic_invalid + another_invalid + new_invalid
    for i in all_valid:
        assert re.fullmatch(pid_caleng, i) is not None
    for i in all_invalid:
        assert re.fullmatch(pid_caleng, i) is None


def test_pid_sci_regexes():
    # First, test out the individual program id variety regexes
    orig_valid = ['GN-2011A-Q-12', 'GS-2000B-C-1', 'GN-2000A-DD-3',
                  'GS-2033B-QS-1234']
    orig_invalid = ['G-2011A-Q-12', 'GS-2000C-C-1', 'GN-2000A-ZZ-3',
                    'GS-2024A-Q-410-36']
    for i in orig_valid:
        assert re.fullmatch(pid_sci_orig, i) is not None
    for i in orig_invalid:
        assert re.fullmatch(pid_sci_orig, i) is None

    new_valid = ['G-2020A-1234-Q', 'G-2020B-1234-C', 'G-2020A-14-F']
    new_invalid = ['GN-2020A-1234-Q', 'G-2020C-1234-C', 'G-2020A-14-Z']
    for i in new_valid:
        assert re.fullmatch(pid_sci_new, i) is not None
    for i in new_invalid:
        assert re.fullmatch(pid_sci_new, i) is None

    # Do the combined test to check for crosstalk
    all_valid = orig_valid + new_valid
    all_invalid = orig_invalid + new_invalid
    for i in all_valid:
        assert re.fullmatch(pid_sci, i) is not None
    for i in all_invalid:
        assert re.fullmatch(pid_sci, i) is None


# These are used for the class tests too
pid_valid = ['GN-CAL20120123', 'CAL20031122', 'GN-2011A-ENG-191',
             'G-2001A-MON-F_2-20', 'GN-2000A-DD-3', 'GN-2011A-Q-12',
             'G-2099B-COM-SCORPIO-1', 'G-2099A-0005-Q', 'G-2012B-1234-P']
pid_invalid = ['GX-CAL20000131', 'Eng20221101', 'GN-2020A-foo-191',
               'GN-2001A-CAL-GMOS_N-20', 'G-2011A-Q-12', 'GN-2020A-1234-Q']


def test_pid_regex():
    # Just a quick sanity check on the uber combined one
    for i in pid_valid:
        assert re.fullmatch(pid, i) is not None
    for i in pid_invalid:
        assert re.fullmatch(pid, i) is None


# Test the GeminiDataLabel class
def test_gdl():
    for pid in pid_valid:
        dl = pid + '-1-002'
        gdl = GeminiDataLabel(dl)
        assert gdl.valid is True
        assert gdl.datalabel == dl
        assert gdl.datalabel_noextension == dl
        assert gdl.program_id == pid
        assert gdl.observation_id == '%s-%s' % (pid, '1')
        assert gdl.dlnum == '002'
    for pid in pid_valid:
        dlnoext = pid + '-3-123'
        dl = dlnoext + '-blah'
        gdl = GeminiDataLabel(dl)
        assert gdl.valid is True
        assert gdl.datalabel == dl
        assert gdl.datalabel_noextension == dlnoext
        assert gdl.program_id == pid
        assert gdl.observation_id == '%s-%s' % (pid, '3')
        assert gdl.dlnum == '123'
        assert gdl.extension == 'blah'
    for pid in pid_invalid:
        dl = pid + '-1-002'
        gdl = GeminiDataLabel(dl)
        assert gdl.valid is False
        assert gdl.datalabel == ''


def test_gobs():
    for pid in pid_valid:
        oid = pid + '-1'
        gobs = GeminiObservation(oid)
        assert gobs.valid is True
        assert gobs.observation_id == oid
        assert isinstance(gobs.program, GeminiProgram)
        assert gobs.program.program_id == pid
        assert gobs.obsnum == '1'
    for pid in pid_invalid:
        oid = pid + '-1'
        gobs = GeminiObservation(oid)
        assert gobs.valid is False
        assert gobs.observation_id == ''


def test_gp():
    for pid in pid_valid:
        gp = GeminiProgram(pid)
        assert gp.valid is True
        assert gp.program_id == pid
        assert gp.is_eng is ('ENG' in pid)
        assert gp.is_cal is ('CAL' in pid)
        assert gp.is_mon is ('MON' in pid)
        assert gp.is_com is ('COM' in pid)
        assert gp.is_q is ('Q' in pid)
        assert gp.is_dd is ('DD' in pid)
        assert gp.is_pw is ('-P' in pid)
        if '2011A' in pid:
            assert gp.semester == '2011A'
        if '20120123' in pid:
            assert gp.date == '20120123'
    for pid in pid_invalid:
        gp = GeminiProgram(pid)
        assert gp.valid is False
        assert gp.program_id is None
        assert gp.is_sv is None
        assert gp.is_com is None

def test_gpp_datalabel():
    # GPP calls for two numeric components to the datalabel to handle
    # instruments that generate multiple files eg from multiple arms
    thing = 'G-2026B-ENG-SCORPIO-02-0023-0011-0002'
    gdl = GeminiDataLabel(thing)
    assert gdl.program_id == 'G-2026B-ENG-SCORPIO-02'
    assert gdl.obsnum == "0023"
    assert gdl.dlnum == "0011-0002"
