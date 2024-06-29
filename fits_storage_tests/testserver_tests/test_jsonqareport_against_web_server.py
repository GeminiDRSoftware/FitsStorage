# These tests test against a running web server. You should stand up a
# fits storage server with a web server running and an empty database
# then run these tests pointing at it.


import requests
import http
from urllib.parse import quote

from fits_storage_tests.testserver_tests.helpers import getserver
from fits_storage_tests.testserver_tests.helpers import _ensureuploaded

json = [
    {
        "hostname": "myhost.gemini.edu",
        "userid": "phirst",
        "processid": 10245,
        "executable": "reduce",
        "software": "QAP",
        "software_version": "1.30",
        "context": "QA",
        "qametric": [
            {
                "datalabel": "GN-2012A-Q-47-55-004",
                "filename": "Z19990203S0123_forStack.fits",
                "detector": "EEV 23451-1, left",
                "iq": {
                    "fwhm": 0.735,
                    "fwhm_std": 0.123,
                    "isofwhm": 0.825,
                    "isofwhm_std": 0.113,
                    "ee50d": 0.935,
                    "ee50d_std": 0.02,
                    "elip": 0.002,
                    "elip_std": 0.001,
                    "pa": 156.3,
                    "pa_std": 30.4,
                    "nsamples": 42,
                    "percentile_band": "IQ70",
                    "adaptive_optics": "True",
                    "ao_seeing": 0.75,
                    "comment": ["faint guide star"]
                },
                "zp":{
                    "mag": 27.432,
                    "mag_std": 0.133,
                    "cloud": 0.23,
                    "cloud_std": 0.05,
                    "photref": "SDSS7",
                    "nsamples": 923,
                    "percentile_band": "CC80",
                    "comment": ["globular cluster"]},
                "sb":{
                    "mag": 22.34,
                    "mag_std": 0.34,
                    "electrons": 4256.2,
                    "electrons_std": 123.4,
                    "nsamples": 2342,
                    "percentile_band": "BGAny",
                    "comment": ["car headlights?"]
                },
                "pe":{
                    "dra": 1.24,
                    "dra_std": 0.03,
                    "ddec": 0.23,
                    "ddec_std": 0.02,
                    "astref": "SDSS7",
                    "nsamples": 23,
                    "comment": ["astrometric field"]
                }
            }
        ]
    }
]


def test_post_qareport():

    url = f"{getserver()}/qareport"
    r = requests.post(url, json=json, timeout=10)

    assert r.status_code == http.HTTPStatus.OK
    assert r.text == ''


def test_xml_qareport():
    xml = "<qareport> </qareport>"
    postdata = quote(xml)

    url = f"{getserver()}/qareport"
    r = requests.post(url, data=postdata, timeout=10)

    # We don't support xml anymore, only json.
    assert r.status_code == http.HTTPStatus.BAD_REQUEST
    assert r.text == ''


def test_qaforgui(tmp_path):
    # Ensure there's a data file there
    _ensureuploaded(tmp_path, 'N20200127S0023.fits.bz2')

    # Hack the json to be a metric for that data file
    json[0]['qametric'][0]['datalabel'] = 'GN-2019B-FT-111-31-001'
    json[0]['qametric'][0]['filename'] = 'N20200127S0023.fits'

    # Upload the hacked json metric
    url = f"{getserver()}/qareport"
    r = requests.post(url, json=json, timeout=10)
    assert r.status_code == http.HTTPStatus.OK
    assert r.text == ''

    # Get the quaforgui report for the data of that data file
    url = f"{getserver()}/qaforgui/20200127"
    r = requests.get(url, timeout=10)
    assert r.status_code == http.HTTPStatus.OK
    result = r.json()

    # Quick sanity check for now
    assert result[0]['metadata']['datalabel'] == 'GN-2019B-FT-111-31-001'
    assert result[0]['iq']['band'] == 'IQ70'
    assert result[0]['cc']['band'] == 'CC80'
    assert result[0]['bg']['band'] == 'BGAny'
