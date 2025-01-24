import requests
import http
import xml.etree.ElementTree as ET

from fits_storage_tests.liveserver_tests.helpers import getserver


def test_sitemap():
    url = f'{getserver()}/sitemap.xml'
    req = requests.get(url)

    assert req.status_code == http.HTTPStatus.OK
    assert req.headers['content-type'] == 'text/xml'

    xml = req.text
    assert len(xml) > 1000

    root = ET.fromstring(xml)

    assert len(root) > 50
