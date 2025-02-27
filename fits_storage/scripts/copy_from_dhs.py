#! /usr/bin/env python3

import smtplib

import sys
import os
import traceback
import datetime
import time
import shutil
import socket

import astrodata
from fits_storage.fits_verify import fitsverify
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.queues.queue import IngestQueue
from fits_storage.logger_dummy import DummyLogger

from fits_storage.gemini_metadata_utils import gemini_date, CHILE_OFFSET
from fits_storage.db import session_scope
from fits_storage.core.orm.diskfile import DiskFile
from fits_storage.core.hashes import md5sum

from fits_storage.config import get_config
fsc = get_config()

"""
Script to copy files from the DHS staging area into Dataflow.
"""


global _today_str
global _yesterday_str

_today_str = gemini_date('today')
_yesterday_str = gemini_date('yesterday')


def get_fake_ut():
    ut = datetime.datetime.utcnow()
    if 'cpo' in socket.gethostname():
        ut -= CHILE_OFFSET
    return ut.date().strftime('%Y%m%d')


class MD5Cache:
    def __init__(self):
        self._today_md5s = dict()
        self._yesterday_md5s = dict()

    def rotate(self):
        """
        Rotate today's list of MD5s into yesterday.

        This moves the 'today' md5 dictionary to yesterday and
        creates a fresh dictionary for today.
        """
        self._yesterday_md5s = self._today_md5s
        self._today_md5s = dict()

    def add_today(self, filename, md5):
        self._today_md5s[filename] = md5

    def add_yesterday(self, filename, md5):
        self._yesterday_md5s[filename] = md5

    def get_md5(self, filename):
        retval = self._today_md5s.get(filename, None)
        if retval is None:
            return self._yesterday_md5s.get(filename, None)
        return retval

    def set_md5(self, filename, md5):
        if _yesterday_str in filename:
            self._yesterday_md5s[filename] = md5
        else:
            self._today_md5s[filename] = md5


_md5_cache = MD5Cache()


def check_md5_differs(filename, logger=DummyLogger()):
    """
    Check if the MD5 of the given source file is different from what we had
    before.

    This is to detect changes in a file.  Because we cache the list of
    recognized files and only clear it out every 1000 iterations, this too
    will only be called every 1000 iterations.  Running md5s on an entire day
    expends about 12 seconds of additional real time.

    Parameters
    ----------
    filename : str
        Name of the file to check

    Returns
    -------
    str md5 checksum if we detect a difference, None if we do not
    """
    if _today_str in filename or _yesterday_str in filename:
        src = os.path.join(fsc.dhs_perm, filename)
        md5 = md5sum(src)
        checkmd5 = _md5_cache.get_md5(filename)
        if checkmd5 is None:
            # If we do not have this file at all, we save the MD5 now.
            _md5_cache.set_md5(filename, md5)
            return None
        elif md5 != checkmd5:
            # Mismatch, we return our calculated md5, but we DO NOT
            # save it in the cache.  We let the copy job save it
            # only if it actually copies the file.
            logger.info('Saw MD5 mismatch for file, signaling for recopy: %s'
                        % filename)
            return md5
    return None


# Utility functions
def check_present(session, filename):
    """
    Check if the file is present as a `fits_storage.orm.DiskFile`

    This function checks in the given session if the filename exists
    in the `fits_storage.orm.DiskFile` table with the `present`
    flag set to True.


    Parameters
    ----------

    session : `sqlalchemy.orm.session.Session`
        SQLAlchemy session to check against
    filename : str
        Name of the file to look for

    Returns
    -------
    True if a record exists in `fits_storage.orm.DiskFile`
    for this filename with `present` set to True
    """
    df = session.query(DiskFile).filter(DiskFile.filename == filename).\
        filter(DiskFile.present == True).first()
    if not df:
        return False
    src = os.path.join(fsc.dhs_perm, filename)
    if df.file_size < os.path.getsize(src):
        return False
    if check_md5_differs(filename):
        # the file changed since we last looked, so we don't have *this* version
        return False
    return True


_seen_validation_failures = dict()
_pending_email = None
_fits_verify_failures = dict()


def validate(fullpath):
    reason = None
    if not fullpath.endswith('.fits'):
        return True
    else:
        try:
            ad = astrodata.open(fullpath)
        except:
            reason = "Unable to open file in astrodata, returning as " \
                     "invalid: %s" % fullpath
        if reason is None:
            try:
                isfits, warnings, errors, report = fitsverify(fullpath)
                if not isfits or errors > 0:
                    if fullpath not in _fits_verify_failures:
                        _fits_verify_failures[fullpath] = 1
                    else:
                        num_verify_failures = _fits_verify_failures[fullpath]
                        _fits_verify_failures[fullpath] = num_verify_failures + 1
                    if _fits_verify_failures[fullpath] < 120:
                        reason = 'File %s failed fitsverify check:\n%s' % (fullpath, report)
                    else:
                        logger.info("%s failed fitsverify 100 times, allowing it now", filename)
            except:
                logger.info("%s unable to run fitsverify, continuing" % filename)
    if reason is not None:
        if fullpath not in _seen_validation_failures:
            _seen_validation_failures[fullpath] = 1
        else:
            num_failures = _seen_validation_failures[fullpath]
            _seen_validation_failures[fullpath] = num_failures+1

        num_failures = _seen_validation_failures[fullpath]

        logger.warn(reason)

        if num_failures == fsc.max_dhs_validation_failures:
            global _pending_email
            if _pending_email is None:
                _pending_email = ''
            _pending_email = '{}{}: {}\n'.format(_pending_email, fullpath, reason)

        return False
    return True


def copy_over(session, iq, logger, filename, dryrun):
    """
    Copy the given file over from DHS and add it to the ingest queue.

    This method copies the given file over from the DHS staging area
    into Dataflow and adds it to the ingest queue.

    Parameters
    ----------

    session : `sqlalchemy.orm.session.Session`
        SQLAlchemy session (unused)
    iq : `fits_storage.queues.queue.IngestQueue`
        Ingest queue to add file to after copying to dataflow
    logger : `logging.logger`
        Logger for log messages
    filename : str
        Name of file to copy
    dryrun : bool
        If True, don't actually copy or add to ingest queue, just check

    Returns
    -------
        True if the file was copied or intentionally ignored (directory,
        known bad, etc.), False if it was not
    """
    src = os.path.join(fsc.dhs_perm, filename)
    dest = os.path.join(fsc.storage_root, filename)
    md5 = check_md5_differs(filename, logger)
    # If the Destination file already exists, skip it
    if os.access(dest, os.F_OK | os.R_OK) \
            and os.path.getsize(dest) >= os.path.getsize(src) \
            and md5 is None:
        # unfortunately, we have to recheck md5 here pending a more invasive refactor
        # I'm hoping we can drop the md5 logic once DHS implements the .part
        logger.info("%s already exists on storage_root - skipping", filename)
        # have to save md5 as a special case, to catch file changes on
        # existing files after starting copy_from_dhs job
        if _today_str in filename or _yesterday_str in filename:
            _md5_cache.set_md5(filename, md5sum(os.path.join(src)))
        return True
    # If the source path is a directory, skip is
    if os.path.isdir(src):
        logger.info("%s is a directory - skipping", filename)
        return True
    # If one of these wierd things, skip it
    if filename in ['.bplusvtoc_internal', '.vtoc_internal']:
        logger.info("%s is a wierd thing - skipping", filename)
        return True
    # If lastmod time is within 5 secs, DHS may still be writing it. Skip it
    lastmod = datetime.datetime.fromtimestamp(os.path.getmtime(src))
    age = datetime.datetime.now() - lastmod
    age = age.total_seconds()
    if age < fsc.min_dhs_age_seconds:
        logger.debug("%s is too new (%.1f)- skipping this time round", filename, age)
        return False
    elif not validate(src):
        logger.debug("%s validation failed, not copying now", filename)
        return False
    else:
        logger.debug("%s age is OK: %.1f seconds", filename, age)
    # OK, try and copy the file over.
    try:
        if dryrun:
            logger.info("Dryrun - not actually copying %s", filename)
        else:
            logger.info("Copying %s to %s", filename, fsc.storage_root)
            # We can't use shutil.copy, because it preserves mode of the
            # source file, making the umask totally useless. Under the hood,
            # copy is just a shutil.copyfile + shutil.copymode. We'll
            # use copyfile instead.
            dst = os.path.join(fsc.storage_root, os.path.basename(src))
            shutil.copyfile(src, dst)
            logger.info("Adding %s to IngestQueue", filename)
            iq.add(filename, '')
            if md5 is not None:
                # NOW we save the md5 of the last version we actually copied
                if _today_str in filename or _yesterday_str in filename:
                    _md5_cache.set_md5(filename, md5)
    except:
        global _pending_email
        logger.error("Problem copying %s to %s", src, fsc.storage_root)
        logger.error("Exception: %s : %s... %s", exc_info=True)
        _pending_email = '{}\nProblem copying {} to {}\n'.format(_pending_email, src, fsc.storage_root)
        _pending_email = '{}\nException: {} :{}... {}\n'.format(_pending_email, sys.exc_info()[0], sys.exc_info()[1],
                                                 traceback.format_tb(sys.exc_info()[2]))

        return False
    # Add it to the ingest queue here
    return True


if __name__ == "__main__":
    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--dryrun", action="store_true", dest="dryrun", default=False, help="Don't actually do anything")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", default=False, help="Run in background mode")

    (options, args) = parser.parse_args()

    # Logging level to debug?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********  copy_from_dhs.py - starting up at %s" % datetime.datetime.now())
    logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

    if fsc.using_s3:
        logger.info("This should not be used with S3 storage. Exiting")
        sys.exit(1)

    logger.info("Doing Initial DHS directory scan...")
    # Get initial DHS directory listing
    dhs_list = set(os.listdir(fsc.dhs_perm))
    logger.info("... found %d files", len(dhs_list))
    known_list = set()

    with session_scope() as session:
        logger.debug("Instantiating IngestQueueUtil object")
        iq = IngestQueue(session, logger=logger)
        logger.info("Starting looping...")
        count = 0  # reset known_list after count of 1000
        while True:
            todo_list = dhs_list - known_list
            logger.info("%d new files to check", len(todo_list))
            for filename in todo_list:
                if 'tmp' in filename or not filename.endswith('.fits'):
                    logger.info("Ignoring tmp file: %s", filename)
                    continue
                filename = os.path.split(filename)[1]
                if check_present(session, filename):
                    logger.debug("%s is already present in database", filename)
                    known_list.add(filename)
                else:
                    if copy_over(session, iq, logger, filename, options.dryrun):
                        known_list.add(filename)

            if _pending_email and fsc.smtp_server:
                subject = "ERROR - copy_from_dhs validation failures"

                mailfrom = 'fitsdata@gemini.edu'
                mailto = ['fitsdata@gemini.edu']

                message = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % (
                    mailfrom, ", ".join(mailto), subject, _pending_email)

                server = smtplib.SMTP(fsc.smtp_server)
                server.sendmail(mailfrom, mailto, message)
                server.quit()

                _pending_email = None

            count = count+1
            if count >= 1000:
                count = 0
                remove_list = set()
                for f in known_list:
                    if _today_str in f or _yesterday_str in f:
                        remove_list.add(f)
                known_list = known_list - remove_list
            if _today_str != get_fake_ut():
                # clear out the md5 cache for a new day
                _md5_cache.rotate()
                _today_str = get_fake_ut()
                _yesterday_str = gemini_date('yesterday')

            logger.debug("Pass complete, sleeping")
            time.sleep(5)
            logger.debug("Re-scanning")
            dhs_list = set(os.listdir(fsc.dhs_perm))
