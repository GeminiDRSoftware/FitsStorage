"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
from FitsStorage import *
from GeminiMetadataUtils import *
import FitsStorageConfig

import os

from mod_python import apache


def upload_processed_cal(req, filename):
  """
  This handles uploading processed calibrations.
  It has to be called via a POST request with a binary data payload
  We drop the data in a staging area, then call a (setuid) script to
  copy it into place and trigger the ingest.
  """

  if(req.method != 'POST'):
    return apache.HTTP_NOT_ACCEPTABLE

  # It's a bit brute force to read all the data in one chunk...
  clientdata = req.read()
  fullfilename = os.path.join(FitsStorageConfig.upload_staging_path, filename)

  f = open(fullfilename, 'w')
  f.write(clientdata)
  f.close()
  clientdata=None

  # Now invoke the setuid ingest program
  command="/opt/FitsStorage/invoke /opt/FitsStorage/ingest_uploaded_calibration.py --filename=%s --demon" % filename
  os.system(command)

  return apache.OK

