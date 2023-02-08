import os
import time

import pytest

from gemini_obs_db import db_config
from gemini_obs_db.utils.gemini_metadata_utils import ratodeg, ratodeg_old, dectodeg, dectodeg_old, GeminiProgram, \
    GeminiDataLabel, GeminiObservation, gemini_date, get_date_offset


def test_ratodeg():
    ra_str = '03:48:30.113'
    ra = ratodeg(ra_str)
    ra_old = ratodeg_old(ra_str)
    assert abs(ra-ra_old) < 0.01
    assert 57.1 < ra < 57.2
    assert 57.1 < ra_old < 57.2


def test_dectodeg():
    dec_str = '+24:20:43.00'
    dec = dectodeg(dec_str)
    dec_old = dectodeg_old(dec_str)
    assert abs(dec-dec_old) < 0.01
    assert 24.3 < dec < 24.4
    assert 24.3 < dec_old < 24.4


def test_program_ids():
    gp = GeminiProgram('G-2020A-Q-123')
    assert gp.is_q
    gp = GeminiProgram('G-2020A-C-123')
    assert gp.is_c

    gp = GeminiProgram('G-2020A-ENG-123')
    assert gp.is_eng
    gp = GeminiProgram('G-2020A-CAL-123')
    assert gp.is_cal
    # this looks not true, cal program sidesteps the subcategories (at least per the original logic)
    # gp = GeminiProgram('G-2020V-CAL-123')
    # assert gp.is_sv
    # gp = GeminiProgram('G-2020F-CAL-123')
    # assert gp.is_ft
    # gp = GeminiProgram('G-2020S-CAL-123')
    # assert gp.is_ds

    gp = GeminiProgram('GN-2020A-SV-123')
    assert gp.is_sv
    gp = GeminiProgram('GN-2020A-FT-123')
    assert gp.is_ft
    gp = GeminiProgram('GN-2020A-DS-123')
    assert gp.is_ds

    gp = GeminiProgram('GN-CAL20200123')
    assert gp.is_cal
    gp = GeminiProgram('GN-ENG20200123')
    assert gp.is_eng

    # test trim of leading 0s for old-style program IDs
    gp = GeminiProgram('GN-2020A-DS-0123')
    assert gp.program_id == 'GN-2020A-DS-123'

    gp = GeminiProgram('GN-2022A-ENG-51')
    assert(gp.is_eng)
    assert(gp.valid)


def test_obsid_as_programid():
    gp = GeminiProgram('GN-2022A-ENG-51-152')
    assert not gp.valid


def test_obsids():
    obs = GeminiObservation('GN-2022A-ENG-51-152')
    assert(obs.valid)
    assert(obs.observation_id == 'GN-2022A-ENG-51-152')


def test_url_parsing_helpers():
    dl = GeminiDataLabel("GN-CAL20220502-5-001")
    assert(dl.projectid == "GN-CAL20220502")
    assert(dl.observation_id == "GN-CAL20220502-5")
    assert(dl.obsnum == "5")
    assert(dl.datalabel == "GN-CAL20220502-5-001")
    assert(dl.dlnum == "001")


def test_get_date_hawaii(monkeypatch):
    monkeypatch.setattr(db_config, 'use_utc', False)
    dt = gemini_date("20220404", offset=get_date_offset(), as_datetime=True)
    assert(dt.year == 2022)
    assert(dt.month == 4)
    assert(dt.day == 4)
    assert(dt.hour == 0)


def test_get_date_chile(monkeypatch):
    save_tz = None
    if 'TZ' in os.environ:
        save_tz = os.environ['TZ']
    os.environ['TZ'] = 'Chile/Santiago'
    time.tzset()
    monkeypatch.setattr(db_config, 'use_utc', False)
    dt = gemini_date("20220404", offset=get_date_offset(), as_datetime=True)
    assert(dt.year == 2022)
    assert(dt.month == 4)
    assert(dt.day == 3)
    assert(dt.hour == 14)
    if save_tz:
        os.environ['TZ'] = save_tz
    else:
        os.environ.unsetenv('TZ')
    time.tzset()


def test_get_date_hawaii_utc(monkeypatch):
    monkeypatch.setattr(db_config, 'use_utc', False)
    dt = gemini_date("20220404Z", offset=get_date_offset(), as_datetime=True)
    assert(dt.year == 2022)
    assert(dt.month == 4)
    assert(dt.day == 4)
    assert(dt.hour == 0)


def test_get_date_chile_utc(monkeypatch):
    save_tz = None
    if 'TZ' in os.environ:
        save_tz = os.environ['TZ']
    os.environ['TZ'] = 'Chile/Santiago'
    time.tzset()
    monkeypatch.setattr(db_config, 'use_utc', False)
    dt = gemini_date("20220404Z", offset=get_date_offset(), as_datetime=True)
    assert(dt.year == 2022)
    assert(dt.month == 4)
    assert(dt.day == 4)
    assert(dt.hour == 0)
    if save_tz:
        os.environ['TZ'] = save_tz
    else:
        os.environ.unsetenv('TZ')
    time.tzset()


if __name__ == "__main__":
    pytest.main()
