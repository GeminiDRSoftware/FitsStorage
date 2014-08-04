"""
This is the FitsVerify module. It provides a python interface to the
fitsverify command.

The FITSVERIFY_BIN string contains the path to the fitsverify
executable
"""

import subprocess
import os
import re

# the path to the fitsverify binary
FITSVERIFY_BIN = '/opt/fitsverify/fitsverify'

def fitsverify(filename):
    """
    Runs the fitsverify command on the filename argument.
    Returns a 4 component array containing

    * a boolean that is true if the argument is a fits file
    * an integer giving the number of warnings (-1 on error)
    * an integer giving the number of errors (-1 on error)
    * a string containing the full fitsverify report or an error message
    """

    # First check that the filename exists is readable and is a file
    exists = os.access(filename, os.F_OK | os.R_OK)
    isfile = os.path.isfile(filename)
    if not(exists and isfile):
        report = "%s is not readable or is not a file" % (filename)
        isfits = False
        warnings = 0
        errors = 1
    else:
        # Fire off the subprocess and capture the output
        subp = subprocess.Popen([FITSVERIFY_BIN, filename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdoutstring, stderrstring) = subp.communicate()

        report = stdoutstring + stderrstring
        # Check to see if we got a not a fits file situation
        if re.search('This does not look like a FITS file.', stdoutstring)
            isfits = False
        else:
            isfits = True

        # If it is a fits file, parse how many warnings and errors we got
        if isfits:
            match = re.search(r'\*\*\*\* Verification found (\d*) warning\(s\) and (\d*) error\(s\). \*\*\*\*', stdoutstring)
            if match:
                warnings = match.group(1)
                errors = match.group(2)
            else:
                report = "Could not match warnings and errors string\n" + stdoutstring + stderrstring
                warnings = -1
                errors = -1

    return [isfits, warnings, errors, report]
