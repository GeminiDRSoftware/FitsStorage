# This is an "Apache Proxy" module, that can be imported in place of mod_python.apache
# If we're not in local mode, it import the real mod_python apache module, if we are in
# local mode, it provides various constants etc (eg apache.OK) that are used in various 
# functions,primarily as return values from functions that generate html and are called 
# directly by the apache request handler.
# Importing this module allows use of those functions without apache mod_python

from FitsStorageConfig import using_apache

if(not using_apache):
  OK = 200
  HTTP_NOT_FOUND = 404
  HTTP_FORBIDDEN = 403
  HTTP_NOT_ACCEPTABLE = 406
  HTTP_NOT_IMPLEMENTED = 501
  HTTP_SERVICE_UNAVAILABLE = 503

else:
  from mod_python.apache import *
