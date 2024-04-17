from fits_storage.server.orm.reduction import Reduction

from fits_storage.core.orm.header import Header
from fits_storage_tests.code_tests.helpers import make_diskfile

def test_reduction(tmp_path):
    # We use this RAW GNIRS file as a basis
    data_file = 'N20180524S0117.fits'

    diskfile = make_diskfile(data_file, tmp_path)
    header = Header(diskfile)

    reduction = Reduction(header, diskfile=diskfile)

    # Should all be default / null / raw data values
    assert reduction.processing_intent is None
    assert reduction.software_mode is None
    assert reduction.software_used is None
    assert reduction.software_version is None
    assert reduction.processing_initiated_by is None
    assert reduction.processing_reviewed_by is None
    assert reduction.processing_review_outcome is None
    assert reduction.processing_level == 0
    assert reduction.processing_tag is None

    # Poke some values and test
    ad = diskfile.get_ad_object
    ad.phu['PROCITNT'] = 'Science-Quality'
    ad.phu['PROCMODE'] = 'Science-Quality'
    ad.phu['PROCSOFT'] = 'Excell'
    ad.phu['PROCSVER'] = '1.2.3'
    ad.phu['PROCINBY'] = 'Mickey Mouse'
    ad.phu['PROCRVBY'] = 'Daffy Duck'
    ad.phu['PROCREVW'] = 'Quick-Look'
    ad.phu['PROCLEVL'] = 3
    ad.phu['PROCTAG'] = 'GOA-1'

    reduction = Reduction(header, diskfile=diskfile)
    assert reduction.processing_intent == 'Science-Quality'
    assert reduction.software_mode == 'Science-Quality'
    assert reduction.software_used == 'Excell'
    assert reduction.software_version == '1.2.3'
    assert reduction.processing_initiated_by == 'Mickey Mouse'
    assert reduction.processing_reviewed_by == 'Daffy Duck'
    assert reduction.processing_review_outcome == 'Quick-Look'
    assert reduction.processing_level == 3
    assert reduction.processing_tag == 'GOA-1'
