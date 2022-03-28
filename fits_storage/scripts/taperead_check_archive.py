import sys
import os
import re
import datetime
import time
import subprocess
import requests
import logging

from gemini_obs_db.db import sessionfactory
from fits_storage import fits_storage_config
from fits_storage.logger import logger, setdebug, setdemon
from fits_storage.orm.tapestuff import TapeRead

# This script loops through entries in the taperead table, and checks them against the archive
# It reports (and by default downloads from the archive) files whose data_md5 in the archive
# is different from the one in the taperead table

archive_cache = {}
cache_hits = 0
cache_misses = 0

def get_archive_md5(filename):
    global archive_cache
    global cache_hits
    global cache_misses

    if filename.endswith('.bz2'):
        compressed = True
        name = filename[:-4]
    else:
        compressed = False
        name = filename

    # Get the data_md5 from the archive for canonical file filename

    # If it's in the cache, delete and return it
    # 0 element is data_md5, 1 is file_md5
    if name in archive_cache:
        cache_hits += 1
        if compressed:
            return archive_cache.pop(name)[1]
        else:
            return archive_cache.pop(name)[0]


    # Cache miss
    cache_misses += 1
    # If the cache is large, start fresh at this point
    if len(archive_cache) > 1000:
        archive_cache = {}

    # If not, get a broad swathe from archive and put in cache
    if name.startswith('N20') or name.startswith('S20'):
        filepre = filename[:9]
    elif name.startswith('GN20') or name.startswith('GS20') or name.startswith('gS20') or name.startswith('gN20'):
        filepre = filename[:8]
    elif name.startswith('img_20'):
        filepre = filename[:12]
    elif name.startswith('mrg'):
        filepre = filename[:10]
    else:
        filepre = name

    url = "https://archive.gemini.edu/jsonfilelist/canonical/filepre=%s" % filepre
    r = requests.get(url)
    if r.status_code != 200:
        return None
    for f in r.json():
        fn = f['filename']
        if fn.endswith('.bz2'):
            fn = fn[:-4]
        archive_cache[fn]= [f['data_md5'], f['file_md5']]

    if name in archive_cache:
        if compressed:
            return archive_cache.pop(name)[1]
        else:
            return archive_cache.pop(name)[0]

    return None
   

def download(file_list):
    # Group in chunks of 480 to avoid download limits
    i = 0
    batch = []
    for f in file_list:
        batch.append(f)
        if len(batch) == 480:
            fetch_batch(batch, i)
            batch = []
            i += 1
    fetch_batch(batch, i)

def fetch_batch(file_list, num):
    # Use a POST download sending the list
    url = "https://archive.gemini.edu/download"
    payload = {'files': file_list}
    filename = "archivedata-%d.tar" % num
    logger.info("Downloading file %s with %d files" % (filename, len(file_list)))
    with requests.post(url, data=payload, stream=True) as r:
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=262144):
                f.write(chunk)

if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually download anything")
    parser.add_option("--requester", action="store", dest="requester", help="Check files for a given requester")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

    (options, args) = parser.parse_args()

    logging.getLogger('requests').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    # Logging level to debug? Include stdio log?
    setdebug(options.debug)
    setdemon(options.demon)

    # Annouce startup
    logger.info("*********    taperead_check_archive.py - starting up at %s" % datetime.datetime.now())

    # Query the DB to get a list of files to check
    session = sessionfactory()
 
    query = session.query(TapeRead)
    if options.requester:
        query = query.filter(TapeRead.requester == options.requester)

    query = query.order_by(TapeRead.filename)

    tapereads = query.all()

    logger.info("Got %d files to check", len(tapereads))

    to_download = []
    problem_files = []
    i=0
    for tr in tapereads:
      logger.debug("Checked %d / %d : %d to download : %d problems; cache size: %d hits: %d misses: %d" % (i, len(tapereads), len(to_download), len(problem_files), len(archive_cache), cache_hits, cache_misses))
      i += 1
      if i % 1000 == 0:
          logger.info("Checked %d / %d : %d to download : %d problems; cache size: %d hits: %d misses: %d" % (i, len(tapereads), len(to_download), len(problem_files), len(archive_cache), cache_hits, cache_misses))
      fn = tr.filename
      if fn.endswith('.bz2'):
          fn = fn[:-4]

      #logger.debug("Checking %s" % fn)
      archive_md5 = get_archive_md5(fn)
      if archive_md5 is None:
          logger.error("Error getting archive md5 for %s" % fn)
          problem_files.append(fn)
          continue

      if archive_md5 == tr.md5:
          logger.debug("File %s matches- taperead md5 %s - archive md5 %s" % (fn, archive_md5, tr.md5))
      else:
          logger.info("File %s differs - taperead md5 %s - archive md5 %s" % (fn, archive_md5, tr.md5))
          to_download.append(fn)

    if options.dryrun:
        logger.info("Dry run - not downloading %d files" % len(to_download))
    else:
        logger.info("Downloading %d files" % len(to_download))
        download(to_download) 

    if len(problem_files):
        logger.info("%d problem files being written to problem_files.txt" % len(problem_files))
        with open("problem_files.txt", "a") as fh:
            for f in problem_files:
                logger.info("Problem File: %s" % f)
                fh.write("%s\n" % f)
    else:
        logger.info("There were no problem files")

    session.close()
    logger.info("taperead archive check complete. Exiting")
