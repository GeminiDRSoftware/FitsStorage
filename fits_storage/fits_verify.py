"""
This is the FitsVerify module. It provides a python interface to the
fitsverify command.
"""

import subprocess
import os
import re
import shutil

def fitsverify(filename, fvpath=None):
    """
    Runs the fitsverify command on the filename argument.
    Returns a 4 component array containing

    * a boolean that is true if the argument is a fits file
    * an integer giving the number of warnings (-1 on error)
    * an integer giving the number of errors (-1 on error)
    * a string containing the full fitsverify report or an error message

    if fvpath evaluates False (None or an empty string) then we search
    $PATH for the fitsverify executable. Otherwise, we assume this value
    is the full path of the fitsverify executable
    """

    # First check that the filename exists is readable and is a file
    exists = os.access(filename, os.F_OK | os.R_OK)
    isfile = os.path.isfile(filename)
    if not(exists and isfile):
        report = "%s is not readable or is not a file" % (filename)
        isfits = False
        warnings = 0
        errors = 1
        return [isfits, warnings, errors, report]


    if not fvpath:
        fvpath = shutil.which('fitsverify')
        if fvpath is None:
            raise OSError("fitsverify executable not found on path")

    # Fire off the subprocess and capture the output
    subp = subprocess.Popen([fvpath, filename],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdoutstring, stderrstring) = subp.communicate()
    report = stdoutstring + stderrstring

    stdoutstring = stdoutstring.decode('utf8', errors='ignore')
    stderrstring = stderrstring.decode('utf8', errors='ignore')

    # Check to see if we got a not a fits file situation
    if re.search('This does not look like a FITS file.', stdoutstring):
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
