#!/usr/bin/env python
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

    These values shall be encoded and not stored as plain ascii text. You will
    need to request these encoded values from Science Users Support Deparment
    (SUSD).

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
# ******************************************************************************
#                       G E M I N I  O B S E R V A T O R Y
# ******************************************************************************
#
#   Script name:        goa2ftp
#
#   Purpose:
#      Provide a command line tool to the Gemini Archive API that allows
#      data requests and associated calibrations to be pulled from GOA
#      and then pushed to a pre-determined account @sftp.gemini.edu.
#
#      This script is tailored to LIGO Followup data distribution to
#      multiple stakeholders on LIGO TOO observations.
#
#   Date               : 2019-03-30
#
#   Author             : Kenneth Anderson
#
#   Requires           : Python 3.x, pysftp, requests
#
#   Modification History:
#    2019-03-30, kanderso : First release, Version 0.1 (d20190330)
#
# Tested with Conda 4.5.11
# ******************************************************************************
import os
import sys
import codecs
import random
import string
import tarfile
import argparse
import subprocess

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
etemplate = """
Dear [PI],

Data for [Program ID] were obtained last night.
The data are available from the Gemini sftp site on the account, {1}.
You can access these data in the following manner:

    $ sftp ligoflow@sftp.gemini.edu
    ligoflow@sftp.gemini.edu's password: 

Enter the account password. File name, {0}, is available in
the LIGO_Followups/ directory. Get the file.

The file you retrieve is a tar-archive and is password protected by a zip layer.
First, unzip the data:

    $ unzip {0}

You will be prompted for the zip password. Enter this (zip password, below)
and then you can execute a normal tar extraction:

    $ tar -xvf {4}

You will see bz2 compressed files and straight uncompressed FITS files. The
'.bz2' files are all associated calibrations for the science data, which are
the uncompressed FITS files in the archive.

sftp account : {1}
sftp password: {2}
zip password : {3}


Regards,

[Observer/QC]
"""
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

 There are two modes of operation:

 1) single file mode -- Single file mode requests a single file and associated
    calibrations, packages these into a tar archive and pushes this to the
    sftp site. If single mode is the desired operation, users must specify the
    --single option on the command line.

 2) package mode -- Package mode is the default operation. This mode pulls all
    science data requested, and all associated calibrations for all files,
    builds a tar archive and password protects the file with a zip layer.
    The password protected file is then pushed to the sftp site. In the default
    package mode, users must provide a name for the data package through
    the --pkgname argument.

 The command line can accept an "at-file" providing the command line arguments.

 E.g.,

    Retrieve files directly on the command line:

        $ goa2ftp --packagename TESTPACK N20170913S0209.fits N20170913S0211.fits

    Or with an "at-file":

        $ goa2ftp --packagename TESTPACK @myFitsFiles

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

  A directory is there called LIGO_Followups/. The tar files are remotely
 transferred to this directory. Users will need to set up access to the sftp
 account and location.

"""
# ------------------------------------------------------------------------------
class RequestError(Exception):
    pass
# ------------------------------------------------------------------------------
def goa2ftp_parser():
    """ The command line. """
    parser = argparse.ArgumentParser(description=dscript, prog='goa2ftp',
                    formatter_class=argparse.RawDescriptionHelpFormatter,
                    fromfile_prefix_chars='@')

    parser.add_argument("-v", "--version", action='version',
                        version='%(prog)s v'+ version)

    parser.add_argument(dest='files', action='store', nargs="+",
                        help="File(s) to retrieve.")

    parser.add_argument("--noftp", dest='nopush', action='store_true',
                        help="Do not push data to SFTP site.")

    group = parser.add_mutually_exclusive_group()

    group.add_argument("--pkgname", dest="pkgname",
                       help="Assign this name to the pushed data package. "
                       "Not applied when --single is specified.")

    group.add_argument("--single", dest="single", action="store_true",
                       help="Retrieve and package science images with "
                       "their associated calibrations individually and "
                       "push to SFTP site.")

    args = parser.parse_args()
    return args

# ------------------------------------------------------------------------------
def emit_message(zname, uname, upass, pwd, pkname):
    print(etemplate.format(zname, uname, upass, pwd, pkname))
    return

def generate_pword(nchars=8):
    chars = string.ascii_letters + string.digits
    random.seed = (os.urandom(1024))
    newpword = ''.join(random.choice(chars) for i in range(nchars))
    return newpword

def zippit(pkgname, pwd, zipname):
    """
    Using the zip utility command. Std library, zipfile, does not seem to
    work well with password protection through the API.

    """
    cmd = []
    cmd.append('zip')
    cmd.append('-P')
    cmd.append(pwd)
    cmd.append(zipname)
    cmd.append(pkgname)
    subprocess.run(cmd)
    return zipname

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
    return (realu, realp)


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
    msg = "  Sending zipped tar archive to {} under {}/{} ..."
    host = 'sftp.gemini.edu'
    ligopath = 'LIGO_Followups'
    user, passwd = get_ftp_credential()
    print()
    print(msg.format(host, user, ligopath))
    with pysftp.Connection(host, username=user, password=passwd) as sftp:
        sftp.chdir(ligopath)
        sftp.put(tfile)
    print("Done.")
    return

def build_datapack(ffiles, pkgname):
    ftar_name = pkgname + ".tar"
    for ffile in ffiles:
        fname = pull_data(ffile)
        tname = pull_cals(ffile)
        tarobj = tarfile.TarFile(name=tname)
        tarobj.extractall()

    newtarb = tarfile.TarFile(name=ftar_name, mode='w')
    packfiles = os.listdir('.')
    for pfs in packfiles:
        if pfs.endswith('.fits') or pfs.endswith('.fits.bz2'):
            newtarb.add(pfs)

    newtarb.close()
    return ftar_name

def push_singles(ffiles, push):
    pulled = []
    for filen in ffiles:
        ffile = pull_data(filen)
        tfile = pull_cals(filen)
        tar_append(tfile, ffile)
        pulled.append((ffile, tfile))
        if push:
            push_tar(tfile)
    return

def sweep():
    files = os.listdir('.')
    for f in files:
        if 'fits' in f:
            os.remove(f)
    return

def main(args):
    if not args.single and not args.pkgname:
        print("\n\tDefault package mode requires a package name.\n"
              "\tPlease provide a package name with --pkgname.\n")
        return

    ffiles = args.files
    print("Pulling file requests.\n\t {}".format(ffiles))
    if args.single:
        push_singles(ffiles, args.nopush)
    else:
        pword = generate_pword()
        print("Building data package ... ")
        pkgname = build_datapack(ffiles, args.pkgname)
        if args.nopush:
            print("Package {} build complete. ".format(pkgname))
            return

        zname = zippit(pkgname, pword, pkgname + '.zip')
        print("zip complete.")
        push_tar(zname)
        uname, upass = get_ftp_credential()
        emit_message(zname, uname, upass, pword, pkgname)

    print("sweeping ...")
    sweep()
    return
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    sys.exit(main(goa2ftp_parser()))
