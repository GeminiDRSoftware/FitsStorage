import datetime
import os

import astrodata
import gemini_instruments
from astrodata.provenance import add_history

from fits_storage.server.orm.provenancehistory import ingest_provenancehistory

from fits_storage_tests.code_tests.helpers import get_test_config, make_diskfile


def test_provenance_provhistory(tmp_path):
    """
    This file has the old PROVHISTORY extension before it was
    renamed to HISTORY
    """
    get_test_config()

    data_file = 'bpm_20220303_gmos-n_Ham_44_full_12amp.fits.bz2'

    diskfile = make_diskfile(data_file, tmp_path)

    # Sanity check the prov and hist in the ad instance
    ad = diskfile.get_ad_object
    assert hasattr(ad, 'PROVENANCE')
    assert hasattr(ad, 'PROVHISTORY')

    provenance = ad.PROVENANCE
    history = ad.PROVHISTORY
    assert len(provenance) == 12
    assert len(history) == 16

    # This doesn't actually ingest per-se - it adds the lists to the diskfile
    # instance, and the ORM would normally take care of actually inserting
    # them into tables via the relationship backref definitions. Here we just
    # check that the lists appear on the diskfile instance.
    ingest_provenancehistory(diskfile)

    assert len(diskfile.provenance) == 12
    assert len(diskfile.history) == 16

    p_zero = diskfile.provenance[0]
    assert p_zero.added_by == 'prepare'
    assert p_zero.filename == 'N20220523S0558.fits'
    assert p_zero.md5 == 'aa6abcfcb5418c2113f8b92571d9cd6a'
    assert p_zero.timestamp == datetime.datetime(2022, 6, 9, 2, 3, 24, 119193)

    h_zero = diskfile.history[0]
    assert h_zero.primitive == 'prepare'
    assert h_zero.args == \
           '{"suffix": "_prepared", "mdf": null, "attach_mdf": true}'
    assert h_zero.timestamp_start == \
           datetime.datetime(2022, 6, 9, 2, 3, 22, 641957)
    assert h_zero.timestamp_end == \
           datetime.datetime(2022, 6, 9, 2, 3, 24, 169567)


def test_provenance_history_conversion(tmp_path):
    """
    This file has the old PROVHISTORY extension before it was
    renamed to HISTORY. Here we test the conversion of that to
    HISTORY by astrodata and then that we can ingest that properly.
    """
    get_test_config()

    data_file = 'bpm_20220303_gmos-n_Ham_44_full_12amp.fits.bz2'

    diskfile = make_diskfile(data_file, tmp_path)

    # Sanity check the prov and hist in the ad instance
    ad = diskfile.get_ad_object
    assert hasattr(ad, 'PROVENANCE')
    assert hasattr(ad, 'PROVHISTORY')

    # Add something to the history, which will trigger converting it from
    # PROVHISTORY to HISTORY
    ts = datetime.datetime.utcnow().isoformat()
    add_history(ad, ts, ts, "primitive_name", "primitive_args")
    assert hasattr(ad, 'PROVENANCE')
    assert not hasattr(ad, 'PROVHISTORY')
    assert hasattr(ad, 'HISTORY')

    provenance = ad.PROVENANCE
    history = ad.HISTORY
    assert len(provenance) == 12
    assert len(history) == 17

    # This doesn't actually ingest per-se - it adds the lists to the diskfile
    # instance, and the ORM would normally take care of actually inserting
    # them into tables via the relationship backref definitions. Here we just
    # check that the lists appear on the diskfile instance.
    ingest_provenancehistory(diskfile)

    assert len(diskfile.provenance) == 12
    assert len(diskfile.history) == 17

    newfile = os.path.join(tmp_path, "testfile.fits")
    ad.write(newfile)
    assert os.path.exists(newfile)

    ad2 = astrodata.open(newfile)
    assert hasattr(ad, 'PROVENANCE')
    assert hasattr(ad, 'HISTORY')
    assert not hasattr(ad, 'PROVHISTORY')

    newhist = ad2.HISTORY[-1]
    assert newhist['timestamp_start'] == ts
    assert newhist['timestamp_stop'] == ts
    assert newhist['primitive'] == 'primitive_name'
    assert newhist['args'] == 'primitive_args'
