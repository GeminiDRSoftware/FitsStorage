"""
This module provides a tape handling class
"""

import sys
import os
import shutil
import subprocess
import tarfile
import re


def get_tape_drive(device, scratchdir):
    if device.lower().startswith("folder:"):
        return FakeTapeDrive(device[7:], scratchdir)
    if device.lower().startswith("sg3utils:"):
        return SG3UtilsTapeDrive(device[9:], scratchdir)
    return TapeDrive(device, scratchdir)


class TapeException(Exception):
    def __init__(self, message, code):
        super().__init__(message, code)
        self.message = message
        self.code = code


class TapeDrive(object):
    """
    This class provides functions to manipulate a Tape Drive
    for the FitsStorage software
    """

    dev = ''
    scratchdir = ''
    workingdir = ''
    origdir = ''
    filenum = ''

    def __init__(self, device, scratchdir):
        """
        initialise the object
        dev is the tape drive device
        scratchdir is a directory we can use for scratch space. This class
        will create a subdir in there with the name being the current pid and
        will operate in that subdir when necessary.
        """
        self.dev = device
        self.scratchdir = scratchdir

    def mkworkingdir(self):
        pid = str(os.getpid())
        self.workingdir = os.path.join(self.scratchdir, pid)
        if not os.path.exists(self.workingdir):
            os.mkdir(self.workingdir)

    def cdworkingdir(self):
        if len(self.workingdir) == 0:
            self.mkworkingdir()
        self.origdir = os.getcwd()
        os.chdir(self.workingdir)

    def cdback(self):
        if self.origdir:
            os.chdir(self.origdir)

    def cleanup(self):
        self.cdback()
        shutil.rmtree(self.workingdir, ignore_errors=True)
        self.workingdir = ''

    def mt(self, mtcmd, mtarg='', fail=False):
        """
        Runs the mt command mtcmd on the tape device with argument mtarg
        Returns the return code from the mt command
        The fail parameter (default False) says whether to print
        an error and exit if the attempt fails
        returns [returncode, stdoutstring, stderrstring]
        """
        cmd = ['/bin/mt', '-f', self.dev, mtcmd]
        if mtarg:
            cmd.append(mtarg)
        sp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdoutstring, stderrstring) = sp.communicate()
        retval = sp.returncode

        if retval and fail:
            print('"mt -f %s %s %s" failed with exit value %d:' % (self.dev, mtcmd, mtarg, retval))
            print(stdoutstring)
            print(stderrstring)
            sys.exit(retval)

        return [retval, stdoutstring, stderrstring]

    def rewind(self, fail=False):
        """
        Rewinds the tape
        The fail argument determines whether to exit with an error if it fails
        Returns the return code from the mt command
        """
        [returncode, stdoutstring, stderrstring] = self.mt('rewind', fail=fail)
        return returncode

    def skipto(self, filenum, fail=True):
        """
        Fast forward the tape to file number filenum
        The fail argument determines whether to exit with an error if it fails
        Returns the return code from the mt command
        """
        if filenum == 0:
            r = self.rewind(fail=fail)
            return r

        if self.fileno(fail=fail) > filenum:
            #print "too far on. Rewinding"
            r = self.rewind(fail=fail)

        num = filenum - self.fileno(fail=fail)
        if num:
            #print "need to move forward %d files" % num
            r = self.fsf(num, fail=fail)

        if self.fileno(fail=fail) == filenum:
            if self.blockno(fail=fail) == 0:
                #print "already there"
                return 0
            else:
                #print "right file, but part way in"
                r = self.bsf(fail=fail)
                r = r + self.fsf(fail=fail)
                return r

    def fsf(self, n=0, fail=True):
        """
        Issue fsf command to tape drive
        """

        if n == 0:
            arg = ''
        else:
            arg = "%d" % n
        [returncode, stdoutstring, stderrstring] = self.mt('fsf', mtarg=arg, fail=fail)
        return returncode

    def bsf(self, n=0, fail=True):
        """
        Issue bsf command to tape drive
        """

        if n == 0:
            arg = ''
        else:
            arg = "%d" % n
        [returncode, stdoutstring, stderrstring] = self.mt('bsf', mtarg=arg, fail=fail)
        return returncode

    def eod(self, fail=True):
        """
        Send the tape to eod
        The fail argument determines whether to exit with an error if it fails
        Returns the return code from the mt command
        """
        [returncode, stdoutstring, stderrstring] = self.mt('eod', fail=fail)
        return returncode

    def setblk0(self, fail=False):
        """
        Calls mt setblk 0 on the tape drive
        The fail argument determines whether to exit with an error if it fails
        Returns the return code from the mt command
        """
        [returncode, stdoutstring, stderrstring] = self.mt('setblk', '0', fail=fail)
        return returncode

    def status(self, fail=False):
        """
        Returns the mt status string, or None if it fails.
        The fail argument determines whether to exit with an error if it fails
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
            tar = tarfile.open(name=self.target(), mode='r|')
            list = tar.getnames()
            tar.close()
            self.rewind()
            if list == ['tapelabel']:
                tar = tarfile.open(name=self.target(), mode='r|')
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
        except:
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

        This function operates unconditionally - it does not check for a pre
        existing label and will simply overwrite the start of the tape.
        It is up to the caller to ensure they really want to do that
        before calling this function
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
            tar = tarfile.open(name=self.target(), mode='w|')
            tar.add('tapelabel')
            tar.close()
            os.unlink('tapelabel')
            self.rewind()
            self.cdback()
            self.cleanup()

        except:
            self.rewind()
            self.cleanup()
            if fail:
                raise

    def write_mode(self):
        return 'w|'

    def target(self):
        return self.dev


class SG3UtilsTapeDrive(TapeDrive):
    def __init__(self, device, scratchdir):
        super().__init__(device, scratchdir)

    def readlabel(self, fail=False):
        """
        Attempt to read a FitsStorage style tape label off the tape
        This implementation uses the sg3utils to get the label, which
        is more efficient than the default implementation
        """
        output = os.popen('sg_ident %s' % self.device).read()
        if os.WEXITSTATUS != 0:
            if fail:
                raise TapeException("Error reading ident from tape, output was: %s" % output, 
                                    os.WEXITSTATUS)
            else:
                return None
        return output

    def writelabel(self, label, fail=True):
        """
        Attempt to write a FitsStorage style tape label to the tape
        This implementation uses the sg3utils to write the label, which
        is more efficient than the default implementation
        """
        output = os.popen('echo "%s" | sg_ident --set %s' % (label, self.device)).read()
        if os.WEXITSTATUS != 0 and fail:
            raise TapeException("Error writing ident to tape, output was: %s" % output, 
                                os.WEXITSTATUS)


class FakeTapeDrive(TapeDrive):
    def __init__(self, device, scratchdir):
        super().__init__(device, scratchdir)
        self.file_no = 0
        self.block_no = 0

    def rewind(self, fail=False):
        self.block_no = 0

    def setblk0(self, fail=False):
        self.block_no = 0

    def online(self):
        return True

    def eod(self, fail=True):
        while True:
            if os.path.exists("%s/block%d" % (self.dev, self.block_no)):
                self.block_no = self.block_no+1
                self.file_no = self.block_no
            else:
                return self.block_no

    def skipto(self, filenum, fail=True):
        self.file_no = filenum
        self.block_no = filenum

    def eot(self):
        return False

    def status(self, fail=False):
        return "File number=%d,block number=%d," % (self.file_no, self.block_no)

    def target(self):
        return "%s/block%d" % (self.dev, self.block_no)