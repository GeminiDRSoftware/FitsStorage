import os.path

from fits_storage_tests.code_tests.helpers import get_test_config

from fits_storage.server.reduce_on_ingest import (ReduceOnIngest)

def test_reduce_on_ingest(tmp_path):
    get_test_config()

    # Write a test rules file to the tmp_path
    rf = os.path.join(tmp_path, 'rules.json')
    rules_json="""[
  [{"instrument": "GMOS-N", "observation_type": "BIAS"},
   {"recipe": "checkBiasOSCO", "capture_monitoring": true, "processing_tag": "GMOS-N_BIAS-1", "initiated_by": "GOA-ingest", "intent": "Science-Quality"}]
]"""
    with open(rf, "w") as f:
        f.write(rules_json)

    roi = ReduceOnIngest(rules_file=rf, session=None)

    assert len(roi.rules) == 1

    r1, a1 = roi.rules[0]
    assert r1.get('instrument') == 'GMOS-N'
    assert a1.get('intent') == 'Science-Quality'
