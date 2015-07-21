import os
import json
import hashlib
import subprocess
import datetime

from fits_storage_config import upload_staging_path, upload_auth_cookie

import apache_return_codes as apache

from orm import sessionfactory
from orm.fileuploadlog import FileUploadLog


if upload_auth_cookie:
    from mod_python import Cookie

def upload_file(req, filename, processed_cal="False"):
    """
    This handles uploading files including processed calibrations.
    It has to be called via a POST request with a binary data payload
    We drop the data in a staging area, then call a (setuid) script to
    copy it into place and trigger the ingest.

    If upload authentication is enabled, the request must contain
    the authentication cookie for the request to be processed.

    Log Entries are inserted into the FileUploadLog table
    """

    session = sessionfactory()
    try:
        fileuploadlog = FileUploadLog(req.usagelog)
        fileuploadlog.filename = filename
        fileuploadlog.processed_cal = processed_cal
        session.add(fileuploadlog)
        session.commit()

        if(req.method != 'POST'):
            fileuploadlog.add_note("Aborted - not HTTP POST")
            return apache.HTTP_NOT_ACCEPTABLE

        authenticated = True
        if upload_auth_cookie:
            authenticated = False
            cookies = Cookie.get_cookies(req)
            if cookies.has_key('gemini_fits_upload_auth'):
                auth_value = cookies['gemini_fits_upload_auth'].value
                if auth_value == upload_auth_cookie:
                    authenticated = True

        if not authenticated:
            fileuploadlog.add_note("Not Authenticated for Upload")
            return apache.HTTP_FORBIDDEN

        # It's a bit brute force to read all the data in one chunk,
        # but that's fine, files are never more than a few hundred MB...
        fileuploadlog.ut_transfer_start = datetime.datetime.utcnow()
        clientdata = req.read()
        fileuploadlog.ut_transfer_complete = datetime.datetime.utcnow()
        fullfilename = os.path.join(upload_staging_path, filename)
    
        f = open(fullfilename, 'w')
        f.write(clientdata)
        f.close()
    
        # compute the md5  and size while we still have the buffer in memory
        m = hashlib.md5()
        m.update(clientdata)
        md5 = m.hexdigest()
        size = len(clientdata)
        fileuploadlog.size = size
        fileuploadlog.md5 = md5
    
        # Free up memory
        clientdata = None

        # Construct the verification dictionary and json encode it
        verification = {'filename': filename, 'size': size, 'md5': md5}
        verif_json = json.dumps([verification])

        # And write that back to the client
        req.write(verif_json)

        # Now invoke the setuid ingest program
        command = ["/opt/FitsStorage/scripts/invoke", "/opt/FitsStorage/scripts/ingest_uploaded_file.py", "--filename=%s" % filename, "--demon", "--processed_cal=%s" % processed_cal, "--fileuploadlog_id=%d" % fileuploadlog.id]

        #ret = subprocess.call(command)
        subp_p = subprocess.Popen(command)
        subp_p.wait()

        ret = subp_p.returncode
        fileuploadlog.invoke_pid = subp_p.pid
        fileuploadlog.invoke_status = subp_p.returncode

        # Because invoke calls execv(), which in turn replaces the process image of the invoke process with that of
        # python running ingest_uploaded_calibration.py, the return value we get acutally comes from that script, not invoke

        if(ret != 0):
            return apache.HTTP_SERVICE_UNAVAILABLE
        else:
            return apache.HTTP_OK

    finally:
        session.commit()
        session.close()
