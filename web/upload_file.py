import os
import subprocess

from fits_storage_config import upload_staging_path

import apache_return_codes as apache

def upload_file(req, filename, processed_cal="False"):
    """
    This handles uploading files including processed calibrations.
    It has to be called via a POST request with a binary data payload
    We drop the data in a staging area, then call a (setuid) script to
    copy it into place and trigger the ingest.
    """

    if(req.method != 'POST'):
        return apache.HTTP_NOT_ACCEPTABLE

    # It's a bit brute force to read all the data in one chunk,
    # but that's fine, files are never more than a few hundred MB...
    clientdata = req.read()
    fullfilename = os.path.join(upload_staging_path, filename)

    f = open(fullfilename, 'w')
    f.write(clientdata)
    f.close()
    clientdata = None

    # Now invoke the setuid ingest program
    command = ["/opt/FitsStorage/scripts/invoke", "/opt/FitsStorage/scripts/ingest_uploaded_file.py", "--filename=%s" % filename, "--demon", "--processed_cal=%s" % processed_cal]

    ret = subprocess.call(command)

    # Because invoke calls execv(), which in turn replaces the process image of the invoke process with that of
    # python running ingest_uploaded_calibration.py, the return value we get acutally comes from that script, not invoke

    if(ret != 0):
        return apache.HTTP_SERVICE_UNAVAILABLE
    else:
        return apache.OK

