import os
import http

import requests


def getserver():
    """
    Returns a server name (in the form https://archive.gemini.edu )
    either using the FITS_STORAGE_TEST_LIVESERVER environment variable,
    or defaulting to archive if that is not set.
    """
    server = os.environ.get('FITS_STORAGE_TEST_LIVESERVER')
    if server is None:
        server = 'https://archive.gemini.edu'

    print(f'Using live server: {server}')
    return server


def calhelper(base_url, dod):
    """
    base_url is the jsoncalmgr url to test against, without the /jsoncalmgr.
    dod is a dict of dicts of the form:
    {'sci_file': {'bias': 'bias_file', 'flat': 'flat_file', ...}, ...}
    where sci_file is a datalabel of a file that should have the
    calibrations associated with it, 'bias', 'flat', ... are calibration types
    and 'bias_file', 'flat_file', ... are the corresponding datalabels of the
    best match (ie first in the results) calibration file.

    For now, we only check one file for each caltype, and it must be first
    in the results list (ie best match). We could expand this to handle lists
    if that becomes useful.

    This function retrieves the associations from base_url/jsoncalmgr for each
    sci_file in cals, and asserts that the first result for each cal type is
    as specified.
    """

    for item in dod.keys():
        print(f"Testing dict item: {item}")
        url = base_url + '/jsoncalmgr/' + item
        req = requests.get(url)
        assert req.status_code == http.HTTPStatus.OK
        results = req.json()

        # There should only be one "science file" result as we specified a
        # single file in the query
        assert len(results) == 1
        result = results[0]

        # ... and it should be the file we asked for.
        assert result['label'] == item

        # Pre-process the cal_info in the results for simplicity
        cals_from_server = {}
        for cal_info_dict in result['cal_info']:
            caltype = cal_info_dict['type']
            calslist = cal_info_dict['cals']
            if calslist is None or len(calslist) == 0:
                label = None
            else:
                label = calslist[0]['label']

            cals_from_server[caltype] = label

        # Now check they match
        for caltype in dod[item].keys():
            print(f"Testing caltype {caltype}")
            assert dod[item][caltype] == cals_from_server[caltype]

def associatedcalhelper(base_url, dod):
    """
    base_url is the associated_cals url to test against, without the
    /associated_cals.
    dod is the same type of dics as for calhelper() (see above)

    For now, we simply check the presence of the cal file in the html results.
    The assocaited_cals html results does not provide any information as to
    the caltype or how the cal was associated, so this is the best we can do.
    This does test the spirit of assocated_cals, and provides a good sanity
    and functional test.

    This function retrieves the html from base_url/associated_cals, and simply
    asserts that the calibration data label is somewhere in the html.
    """

    for item in dod.keys():
        print(f"Testing dict item: {item}")
        url = base_url + '/associated_cals/' + item
        req = requests.get(url)
        assert req.status_code == http.HTTPStatus.OK
        html = req.text

        for caltype in dod[item].keys():
            if dod[item][caltype] is not None:
                assert dod[item][caltype] in html


def fetch_helper(base_url, filename, buzzwords):
    """
    base_url is the base URL to test against, we'll append /filename to it,
    and fetch that URL. We then assert that we got a 200 status, and we also
    assert that each item in the buzzwords list is in the text returned.

    This is pretty crude in terms of analysing returned html (for example),
    but it provides a good sanity check in many cases.
    """

    url = base_url + '/' + filename
    req = requests.get(url)
    assert req.status_code == http.HTTPStatus.OK
    results = req.text

    for word in buzzwords:
        assert word in results
