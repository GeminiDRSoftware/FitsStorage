import http

import requests


def calhelper(base_url, dod):
    """
    base_url is the jsoncalmgr url to test against. dod is a dict of dicts of
    this form: {'sci_file': {'bias': 'bias_file', 'flat': 'flat_file', ...},
    ...} where sci_file is a datalabel of a file that should have the
    calibations associated with it, 'bias', 'flat', ... are calibration types
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
        url = base_url + '/' + item
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
            assert dod[item][caltype] == cals_from_server[caltype]


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
