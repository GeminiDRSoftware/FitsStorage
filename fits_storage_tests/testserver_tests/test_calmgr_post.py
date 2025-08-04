import requests
import os
import json

from fits_storage_tests.testserver_tests.helpers import getserver
from fits_storage_tests.testserver_tests.helpers import _ensureuploaded

from fits_storage_tests.code_tests.helpers import fetch_file

from recipe_system.cal_service.calrequestlib import get_descriptors_dict
from recipe_system.cal_service.remotedb import make_dict_json_encodable

import astrodata
import gemini_instruments

def test_jsoncalmgr_post(tmp_path):
    filename = 'N20220108S0234.fits.bz2'
    _ensureuploaded(tmp_path, filename)

    fetch_file(filename, tmp_path)
    fpfn = os.path.join(tmp_path, filename)
    ad = astrodata.open(fpfn)

    tags = list(ad.tags)
    descriptors = make_dict_json_encodable(get_descriptors_dict(ad))

    payload = {'tags': tags, 'descriptors': descriptors}
    jsontext = json.dumps(payload)

    server = getserver()
    url = f"{server}/jsoncalmgr/bias"

    r = requests.post(url, data=jsontext, timeout=10)

    assert r.status_code == 200
    print(r.text)

    payload = r.json()

    assert payload[0]['label'] == 'GN-CAL20220108-4-086'
    assert payload[0]['cal_info'][0]['type'] == 'bias'
    assert payload[0]['cal_info'][0]['cals'][0]['label'] == 'GN-CAL20220108-4-086'

