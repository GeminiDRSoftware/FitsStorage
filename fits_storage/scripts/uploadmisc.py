import os
from optparse import OptionParser

import sys
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

from fits_storage.logger import logger, setdebug

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--url", action="store", type="string", dest="url", help="URL to post to, such as https://archive.gemini.edu/miscfiles")
    parser.add_option("--program", action="store", type="string", dest="program", help="Program ID")
    parser.add_option("--filename", action="store", type="string", dest="filename", help="File to upload")
    parser.add_option("--description", action="store", type="string", dest="description", help="File containing description")
    parser.add_option("--release", action="store", type="string", dest="uploadRelease", help="Release (i.e. 'now')")
    # Cookie is a bit of a hack, until we rework miscfiles end point to not required a user login
    parser.add_option("--cookie", action="store", type="string", dest="cookie", help="Cookie for user to execute as on server")

    (options, args) = parser.parse_args()

    # Logging level to debug? Include stdio log?
    setdebug(options.debug)

    url = options.url
    program = options.program
    filename = options.filename
    description = options.description
    release = options.release
    cookie = options.cookie

    if not url:
        logger.error("Must specify a url")
        exit(1)
    if not program:
        logger.error("Must specify a program")
        exit(1)
    if not filename:
        logger.error("Must specify file to upload")
        exit(1)
    if not description:
        logger.error("Must specify file with description text")
        exit(1)
    if not release:
        logger.error("Must specify release")
        exit(1)
    if release != "now":
        logger.error("Currently only supporting release of 'now'")
        exit(1)
    if not cookie:
        logger.error("Cookie required to get permission to upload")
        exit(1)
    if os.isfile(filename):
        logger.error("File %s not found" % filename)
    if not os.isfile(description):
        logger.error("Description File %s not found" % description)

    uploadFile = filename
    uploadDesc = description

    desc = ''
    with open('foo.txt') as txt:
        desc = txt.read()

    mp_encoder = MultipartEncoder(
        fields = {
            'upload': 'True',
            'uploadFile': (uploadFile, open(uploadFile, 'rb'), 'application/x-gzip'),
            'uploadRelease': 'now',
            'uploadProg': program,
            'uploadDesc': desc,
        }
    )

    cookies = {'gemini_archive_session': cookie}

    r = requests.post(url, data=mp_encoder, cookies=cookies, headers={'Content-Type': mp_encoder.content_type})
