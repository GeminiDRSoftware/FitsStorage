#!/usr/bin/env python
# +------------------- Gemini Observatory --------------------+
# |                       gemaux_python                       |
# |                          goa_pull                         |
# |                   Version 0.1, d20190330                  |
# |                                                           |
# |               Requires Py3, pysftp, requests              |
# |                  Tested with Conda 4.5.11                 |
# |             Gemini Observatory, La Serena, Cl             |
# |    Please use the help desk for submission of questions   |
# |  http://www.gemini.edu/sciops/helpdesk/helpdeskIndex.html |
# |     You can also check the Data Reduction User Forum      |
# |                 http://drforum.gemini.edu                 |
# +-----------------------------------------------------------+
"""
Request data from GOA, including associated calibrations. Transfer responsive
data to the Gemini sftp site and to an established user account to accept these
data.

To ensure this program works as expected, two credentials are needed:

    1 -- Gemini Archive authority, gained through an archive session cookie.
    2 -- SFTP username and password.

This script expects to acquire these authorities from the user's home directory.

(1) GOA archive authority is acquired through the arrangement as follows:

    A user's home directory shall have a path to a '.auth' file as follows:

    ${ARCHAUTHORITY}/.auth

    the contents of which shall be, and only shall be, a GOA session cookie.

      ${ARCHAUTHORITY}/.auth/:
      ------------
      <big-long-string-of-stuff>

    This path, and especially the '.auth' file, should be read restricted to
    user only. Once this is set up, set the environment variable:

    export ARCHAUTHORITY=~/.goa
    setenv ARCHAUTHORITY ~/.goa  (csh/tcsh)

    By convention, it is advised to use the dot directory, .goa, as your
    ARCHAUTHORITY.

(2) SFTP authority is is acquired through the arrangement as follows:

      ${SFTPAUTHORITY}/credentials/.ligo

    The contents of which shall be, and only shall be, the encoded username and
    password in the '.ligo' file. It should all look like,

      ${SFTPAUTHORITY}/credential/.ligo:
      ---------------------------------
      <username>
      <password>

    These values shall be encoded and not stored as plain ascii text. You will need
    to request these encoded values from the Science Users Support Deparment (SUSD).

    Once this is set up, set an environment variable named, 'SFTPAUTHORITY':

    export SFTPAUTHORITY=~/.sftp
    setenv SFTPAUTHORITY ~/.sftp  (csh/tcsh)

    By convention, it is advised to use the dot directory, .sftp, as your
    SFTPAUTHORITY.

    Users should set directory and file permissions for user access
    only. Be as restrictive as possible.
    E.g.,

    -r--------   1 username  group   344 Mar 21 12:31 .auth

NOTE: The 'credentials' directory exists in order to accommodate more than
      one sftp account, but a '.ligo' file is the only credential for the SFTP
      site. It exists specifically for LIGO follow-up data. There is currently
      no API or command line option to change this account, but there can be
      if and when such a need arises.

This program is currently tuned to retrieve and push LIGO-followup data.
The code can be generalized, if needs arise.

"""
import os
import sys
import codecs
import tarfile
import argparse

from urllib.parse import urlunparse
from urllib.request import Request
from urllib.request import urlopen

import pysftp
import requests

from requests.exceptions import HTTPError
from requests.exceptions import Timeout
from requests.exceptions import ConnectionError

# ------------------------------------------------------------------------------
version = "0.1 (d20190320)"
# ------------------------------------------------------------------------------
arch_authority = 'ARCHAUTHORITY'
arch_key = '.auth'
sftp_authority = 'SFTPAUTHORITY'
sftp_key = '.ligo'
# ------------------------------------------------------------------------------
scheme = 'https'
netloc = 'archive.gemini.edu'
fpath = 'file/{}'
calpath = 'download/associated_calibrations/{}/notengineeering/canonical/NotFail'
filplate = urlunparse((scheme, netloc, fpath, None, None, None))
calplate = urlunparse((scheme, netloc, calpath, None, None, None))
# ------------------------------------------------------------------------------
CHUNK_SIZE = 1 << 20
# ------------------------------------------------------------------------------
dscript = """
Description:
 The program will receive one or more FITS file names to be retrieved from Gemini
 Observatory Archive (GOA). The program retrieves the specified files, requests
 associated calibrations for those files, bundles the data into a tarball, then
 puts the data product onto the Gemini sftp site and under the user account,
 'ligoflow'. Users will need to set up GOA access authority and SFTP credentials,
 instructions for which are beyond the scope of this 'help'. An external document
 is available.

 The command line can accept an "at-file" providing the command line arguments.

 E.g.,

    Retrieve files directly on the command line:

        $ goa_pull N20170913S0209.fits N20170913S0211.fits [ ... ]

    Or with an "at-file":

        $ goa_pull @myFitsFiles

 where 'myFitsFiles' is a plain text file specifying the FITS file names to be
 retreived from GOA.

    myFitsFiles:
    ------------
    N20170913S0209.fits
    N20170913S0211.fits

 Once retrieved, a query for associated calibrations is performed, results are 
 returned as a tar file. The contents of this tarfile will be bzip2 FITS files
 (.fits.bzp2)

 The returned science dataset (i.e. here N20190321S0295.fits) is appended to
 the calibration tarfile, but is intentionally left uncompressed. This provides
 a quick identifier of the science image -- the uncompressed FITS file
 (extension, .fits) and the calibration files (.fits.bz2).

 The tarfile is named after the requested science data, like,
 N20190321S0295_assoc_cals.tar, to indicate that the tar file has both the
 requested data and the calibrations.

 An account on sftp.gemini.edu has been established, username 'ligoflow'.
 A subdirectory is there called LIGO_Followups/. The tar files are remotely
 transferred to this directory. Users will need to set up access to the sftp
 account and location.

"""
class RequestError(Exception):
    pass

def pull_parser():
    """ The command line. """
    parser = argparse.ArgumentParser(description=dscript, prog='goa_pull',
                    formatter_class=argparse.RawDescriptionHelpFormatter,
                    fromfile_prefix_chars='@')

    parser.add_argument("-v", "--version", action='version',
                        version='%(prog)s v'+ version)
    parser.add_argument(dest='files', action='store', nargs="+",
                        help="File(s) to retrieve.")

    args = parser.parse_args()
    return args

def get_goa_authority():
    goa = os.environ.get(arch_authority)
    try:
        assert goa
    except AssertionError:
        err = "GOA credential not found"
        raise EnvironmentError(err)
    keyfile = os.path.join(goa, arch_key)

    try:
        assert os.path.isfile(keyfile)
    except AssertionError:
        err = "GOA credential not found"
        raise EnvironmentError(err)

    with open(keyfile) as f:
        arch_session = f.read()

    return arch_session

def get_ftp_credential():
    sftp = os.environ.get(sftp_authority)
    try:
        assert sftp
    except AssertionError:
        err = "SFTP credential not found"
        raise EnvironmentError(err)
    keyfile = os.path.join(sftp, 'credentials', sftp_key)

    try:
        assert os.path.isfile(keyfile)
    except AssertionError:
        err = "SFTP credential not found"
        raise EnvironmentError(err)

    with open(keyfile) as f:
        u, p = f.read().split()
        realu = codecs.decode(u.encode(), 'hex_codec').decode()
        realp = codecs.decode(p.encode(), 'hex_codec').decode()
    return (realu,realp)


def get_cal_request(url):
    r = requests.get(url, timeout=10.0)
    try:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=128):
            yield chunk
    except HTTPError as err:
        raise RequestError(["Could not retrieve {}".format(url), str(err)])
    except ConnectionError as err:
        raise RequestError(["Unable to connect to {}".format(url), str(err)])
    except Timeout as terr:
        raise RequestError(["Request timed out", str(terr)])


def form_tarname(fid):
    head, tail = os.path.splitext(fid)
    otar = "{}_assoc_cals.tar".format(head)
    return otar

def form_url(filen):
    urlrequest = filplate.format(filen)
    return urlrequest

def form_assoc_cals_url(filen):
    return calplate.format(filen)

def make_request(url):
    arch_session = get_goa_authority()
    request = Request(url)
    arch_cookie = 'gemini_archive_session={}'.format(arch_session)
    request.add_header('Cookie', arch_cookie)
    openurl = urlopen(request, timeout=30)
    return openurl


def pull_cals(filen):
    tarball = form_tarname(filen)
    cals_url = form_assoc_cals_url(filen)
    print("Request made on URL:\n\t {}".format(cals_url))
    with open(tarball, 'wb') as tarb:
        for chunk in get_cal_request(cals_url):
            tarb.write(chunk)
    return tarball


def pull_data(filename):
    download_url = form_url(filename)
    print("Request made on URL:\n\t {}".format(download_url))
    conn = make_request(download_url)
    newf = conn.read()
    with open(filename, 'wb') as pulled_fits:
        pulled_fits.write(newf)
    return filename

def tar_append(tball, ffile):
    with tarfile.open(tball, 'a') as tob:
        tob.add(ffile)
    print("Added science frame {} to tar archive, {}".format(ffile, tball))
    return

def push_tar(tfile, ppath=None):
    host = 'sftp.gemini.edu'
    user, passwd = get_ftp_credential()
    ligopath = 'LIGO_Followups'
    putpath = ppath
    print()
    print("  Sending tar to {} under {}/{} ...".format(host, user, ligopath))
    with pysftp.Connection(host, username=user, password=passwd) as sftp:
        sftp.chdir(ligopath)
        sftp.put(tfile)
    print("Done.")
    return

def main(args):
    ffiles = args.files
    pulled = []
    print("Pulling file requests.\n\t {}".format(ffiles))
    for filen in ffiles:
        ffile = pull_data(filen)
        tfile = pull_cals(filen)
        tar_append(tfile, ffile)
        pulled.append((ffile, tfile))
        push_tar(tfile)
    return pulled
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    sys.exit(main(pull_parser()))
