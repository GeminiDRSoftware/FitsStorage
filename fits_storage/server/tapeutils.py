"""
This module provides a tape drive handling class, and some utility classes to
help with the tapeserver api.
"""
import http
import sys
import os
import shutil
import subprocess
import tarfile
import re

import requests

from fits_storage.config import get_config


class TapeDrive(object):
    """
    This class provides functions to manipulate a Tape Drive
    for the FitsStorage software
    """

    dev = None
    scratchdir = None
    workingdir = None
    origdir = None
    filenum = None

    def __init__(self, device, scratchdir):
        """
        dev is the tape drive device, scratchdir is a directory we can use
        for scratch space. This class will create a subdir in scratchdir with
        the name being the current pid and will operate in that subdir when
        necessary.
        """
        self.dev = device
        self.scratchdir = scratchdir

    def mkworkingdir(self):
        pid = str(os.getpid())
        self.workingdir = os.path.join(self.scratchdir, pid)
        if not os.path.exists(self.workingdir):
            os.mkdir(self.workingdir)

    def cdworkingdir(self):
        if self.workingdir is None:
            self.mkworkingdir()
        self.origdir = os.getcwd()
        os.chdir(self.workingdir)

    def cdback(self):
        if self.origdir:
            os.chdir(self.origdir)

    def cleanup(self):
        self.cdback()
        shutil.rmtree(self.workingdir, ignore_errors=True)
        self.workingdir = None

    def mt(self, mtcmd, mtarg='', fail=False):
        """
        Runs the mt command mtcmd on the tape device, with argument mtarg
        i.e. mt -f self.dev mtcmd [mtarg]
        Returns the return code from the mt command
        The fail parameter (default False) says whether to print
        an error and exit if the attempt fails
        returns [returncode, stdoutstring, stderrstring]
        """
        cmd = ['/bin/mt', '-f', self.dev, mtcmd]
        if mtarg:
            cmd.append(mtarg)
        sp = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        (stdoutstring, stderrstring) = sp.communicate()
        retval = sp.returncode

        if retval and fail:
            print('"mt -f %s %s %s" failed with exit value %d:' %
                  (self.dev, mtcmd, mtarg, retval))
            print(stdoutstring)
            print(stderrstring)
            sys.exit(retval)

        return [retval, stdoutstring, stderrstring]

    def rewind(self, fail=False):
        """
        Rewinds the tape
        if fail=True, hard exit with an error if it fails
        Returns the return code from the mt command
        """
        [returncode, stdoutstring, stderrstring] = self.mt('rewind', fail=fail)
        return returncode

    def skipto(self, filenum, fail=True):
        """
        Fast-forward the tape to file number filenum
        if fail=True, hard exit with an error if it fails
        Returns the return code from the last mt command
        """
        r = 0

        if filenum == 0:
            r = self.rewind(fail=fail)
            return r

        if self.fileno(fail=fail) > filenum:
            # print "too far on. Rewinding"
            r = self.rewind(fail=fail)

        num = filenum - self.fileno(fail=fail)
        if num:
            # print "need to move forward %d files" % num
            r = self.fsf(num, fail=fail)

        if self.fileno(fail=fail) == filenum:
            if self.blockno(fail=fail) == 0:
                # print "already there"
                pass
            else:
                # print "right file, but part way in"
                self.bsf(fail=fail)
                r = self.fsf(fail=fail)
            return r

    def fsf(self, n=0, fail=True):
        """
        Issue fsf command to tape drive
        """

        if n == 0:
            arg = ''
        else:
            arg = "%d" % n
        [returncode, stdoutstring, stderrstring] = \
            self.mt('fsf', mtarg=arg, fail=fail)
        return returncode

    def bsf(self, n=0, fail=True):
        """
        Issue bsf command to tape drive
        """

        if n == 0:
            arg = ''
        else:
            arg = "%d" % n
        [returncode, stdoutstring, stderrstring] = \
            self.mt('bsf', mtarg=arg, fail=fail)
        return returncode

    def eod(self, fail=True):
        """
        Send the tape to eod
        if fail=True, hard exit with an error if it fails
        Returns the return code from the mt command
        """
        [returncode, stdoutstring, stderrstring] = self.mt('eod', fail=fail)
        return returncode

    def setblk0(self, fail=False):
        """
        Calls mt setblk 0 on the tape drive
        if fail=True, hard exit with an error if it fails
        Returns the return code from the mt command
        """
        [returncode, stdoutstring, stderrstring] = \
            self.mt('setblk', '0', fail=fail)
        return returncode

    def status(self, fail=False):
        """
        Returns the mt status string, or None if it fails.
        if fail=True, hard exit with an error if it fails
        """
        retval = None
        [returncode, stdoutstring, stderrstring] = self.mt('status', fail=fail)
        if returncode == 0:
            retval = stdoutstring.decode('ascii')

        return retval

    def online(self):
        """
        returns True if the tape drive is online
        returns False otherwise
        """
        string = self.status()
        if re.search('ONLINE', string):
            return True
        else:
            return False

    def eot(self):
        """
        returns True if the tape is at EOT (End Of Tape)
        returns False otherwise
        """
        string = self.status()
        if re.search('EOT', string):
            retval = True
        else:
            retval = False
        return retval

    def fileno(self, fail=False):
        """
        Returns the file number the drive is currently
        positioned at
        """
        retval = None
        string = self.status(fail=fail)
        match = re.search(r'(File number=)(\d+)(,)', string)
        if match:
            retval = int(match.group(2))

        return retval

    def blockno(self, fail=False):
        """
        Returns the block number within the file the drive is currently
        positioned at
        """
        retval = None
        string = self.status(fail=fail)
        match = re.search(r'(block number=)(\d+)(,)', string)
        if match:
            retval = int(match.group(2))

        return retval

    def readlabel(self, fail=False):
        """
        Attempt to read a FitsStorage style tape label off the tape
        scratchdir is a directory we can write in. This function
        will operate in a subdirectory in there named with the current pid
        The fail parameter says whether to exit with an error if it fails.
        Returns the tape label string, or raises an error if it fails
        """
        retval = None

        try:
            self.rewind()
            self.setblk0()
            tar = tarfile.open(name=self.dev, mode='r|')
            tarnames = tar.getnames()
            tar.close()
            self.rewind()
            if tarnames == ['tapelabel']:
                tar = tarfile.open(name=self.dev, mode='r|')
                self.cdworkingdir()
                tar.extractall()
                tar.close()
                self.rewind()
                labfile = open('tapelabel', 'r')
                retval = labfile.readline().strip()
                labfile.close()
                os.unlink('tapelabel')
                self.cdback()
                self.cleanup()
        except Exception:
            self.rewind()
            if fail:
                raise

        return retval

    def writelabel(self, label, fail=True):
        """
        Writes a FitsStorage tape label to the start of the tape.
        scratchdir is a directory we can write in. This function
        will operate in a subdirectory in there named with the current pid
        The fail parameter says whether to exit with an error if it fails.

        This function operates unconditionally - it does not check for a
        pre-existing label and will simply overwrite the start of the tape.
        It is up to the caller to ensure they really want to do that before
        calling this function
        """

        try:
            self.rewind()
            self.setblk0()
            self.cdworkingdir()
            if os.access('tapelabel', os.F_OK):
                os.unlink('tapelabel')
            f = open('tapelabel', 'w')
            f.write(label)
            f.close()
            tar = tarfile.open(name=self.dev(), mode='w|')
            tar.add('tapelabel')
            tar.close()
            os.unlink('tapelabel')
            self.rewind()
            self.cdback()
            self.cleanup()

        except Exception:
            self.rewind()
            self.cleanup()
            if fail:
                raise


class FileOnTapeHelper(object):
    """
    This class streamlines querying if a file is on tape via the API. It
    provides a certain amount of local caching to reduce the number of
    times you need to hit the API.

    You should instantiate this class once before looping through files, then
    call the methods on it as appropriate for each file. Be aware if using this
    in long-running processes that the cache can grow huge.
    """
    tapeserver = None
    _cache = None
    _queried = None
    reqses = None

    def __init__(self, tapeserver=None):
        fsc = get_config()
        self.reqses = requests.Session()
        self.tapeserver = fsc.tapeserver if tapeserver is None else tapeserver
        self._cache = []
        self._queried = []

    def query_api(self, filepre):
        """
        Query the server and return a list of dictionaries in the same format
        as the cache. The cache is a list of dictionaries. This may not be most
        efficient, but it's simple and robust.
        [{'filename': 'file.fits.bz2', 'trimmed_filename': 'file.fits',
        'data_md5': abc123, 'tape_id': 123, 'tape_set': 321}, ...]
        """
        results = []
        self._queried.append(filepre)
        url = f"http://{self.tapeserver}/jsontapefile/{filepre}"
        req = self.reqses.get(url)
        if req.status_code != http.HTTPStatus.OK:
            return None
        for i in req.json():
            # We add a trimmed_filename to each entry now for efficiency
            i['trimmed_filename'] = i['filename'].removesuffix(".bz2")
            results.append(i)
        return results

    def populate_cache(self, filepre):
        results = self.query_api(filepre)
        if results:
            self._cache.extend(results)
            return True
        else:
            return False

    def make_filepre(self, filename):
        """
        This generates a filepre from a filename, to trigger a certain
        amount of read-ahead when we query for a specific file. ie if we're
        looking for N20201122S1234.fits, we'll actually query all of
        N20201122 so that subsequent queries for files on the same night will
        be in the cache.
        """
        if filename.startswith('N20') or filename.startswith('S20'):
            return filename[:9]
        if filename.startswith('GN20') or filename.startswith('GS20') or \
                filename.startswith('gN20') or filename.startswith('gS20'):
            return filename[:8]
        if filename.startswith('img_20'):
            return filename[:12]
        if filename.startswith('mrg'):
            return filename[:10]
        if filename.startswith('SDC'):
            return filename[:13]
        return filename

    def check_results(self, filename, data_md5=None, api_results=None):
        """
        Check of a filename / data_md5 combination in a list of api results
        that is in the cache format. If api_results is None, we use the
        cache. If data_md5 is None, we don't match on md5.
        Returns a set containing the tape_ids the file is on.
        """
        tape_ids = set()
        api_results = api_results if api_results else self._cache
        tfilename = filename.removesuffix('.bz2')

        for item in api_results:
            if item['trimmed_filename'] == tfilename:
                if (data_md5 is None)\
                        or (data_md5 == item['data_md5']):
                    tape_ids.add(item['tape_id'])
        return tape_ids

    def check_file(self, filename, data_md5=None):
        """
        Check if a file is on tape. First check the cache. If cache miss,
        form a filepre for a direct query and check if we queried it already.
        If not, query it, check those results, and add them to the cache.
        Return the set of tape_ids the file is on.
        """

        # Check the cache
        cache_results = self.check_results(filename, data_md5)
        if cache_results:
            return cache_results

        # Cache miss, get a filepre and query it
        filepre = self.make_filepre(filename)
        if filepre in self._queried:
            # Already checked, there is none
            return set()
        else:
            api_results = self.query_api(filepre)
            new_results = self.check_results(filename, data_md5, api_results)
            self._cache.extend(api_results)
            return new_results
