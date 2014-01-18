"""
This is the Cadc module. It provides a python interface to various CADC / GSA 
funcitons
"""

import subprocess
import os
import re
import urllib2
import dateutil.parser

def get_gsa_info(filename, user, passwd):
    """
    Queries the GSA for the given filename, using authentication details provided.
    returns a dict containing md5sum and ingestdate keys
    """

    # The base URL
    url = 'http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/data/auth/GEMINI'

    # Create an authenticated urllib2 request object set to do http HEAD
    class HeadRequest(urllib2.Request):
        def get_method(self):
            return "HEAD"
    auth_handler = urllib2.HTTPBasicAuthHandler()
    auth_handler.add_password(realm='Canadian Astronomy Data Centre', uri=url, user=user, passwd=passwd)
    opener = urllib2.build_opener(auth_handler)
    urllib2.install_opener(opener)

    # Form the complete URL and get the http header. Doesn't fetch the actual data
    url = '%s/%s' % (url, filename)

    request = HeadRequest(url)
    try:
        response = urllib2.urlopen(request, timeout=30)
        headers = response.info()
        response.close()
        http_error = 0
    except urllib2.HTTPError:
        http_error = 1

    # Make the empty return dictionary
    dict = {}

    if http_error:
        dict['md5sum'] = None
        dict['ingestdate'] = None
    else:
        # Put the MD5sum from the header into the dict
        dict['md5sum'] = response.headers.get('X-Uncompressed-MD5')

        # Get the ingest date string and parse it into the dict
        ids = response.headers.get('Last-Modified')
        id = dateutil.parser.parse(ids)
        dict['ingestdate'] = id

    return dict

# the array containing the executable and parameters for the wmd program

wmd = ['/astro/i686/jre1.5.0_03/bin/java', '-Djava.library.path=/opt/cadc/mdIngest/lib.x86_fedora', '-Dca.nrc.cadc.configDir=/opt/cadc/mdIngest/config', '-jar', '/opt/cadc/mdIngest/lib/mdIngest.jar', '--archive=GEMINI', '-c', '-d', '--log=/dev/null']

# Compile the regular expression here for efficiency
cre = re.compile('File \S* (IS|IS NOT) ready for ingestion')

def cadcWMD(filename):
    """
    Run the wmd command on filename.
    Returns a (boolean, string) pair. 
    The boolean is true if wmd says the file IS ready to be ingested 
    into the GSA, and false if wmd says IS NOT ready for ingestion. 
    The string is the text output of the wmd report.
    """
    
    # First check that the filename exists is readable and is a file
    exists = os.access(filename, os.F_OK | os.R_OK)
    isfile = os.path.isfile(filename)
    if(not(exists and isfile)):
        print "%s is not readable or is not a file" % (filename)
        return

    wmd_arg = "--file=%s" % filename
    wmdcmd = list(wmd)
    wmdcmd.append(wmd_arg)

    env = os.environ
    env['LD_LIBRARY_PATH'] = '/opt/cadc/mdIngest/lib.x86_fedora'
    # Fire off the subprocess and capture the output
    sp = subprocess.Popen(wmdcmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdoutstring, stderrstring) = sp.communicate()

    match = cre.search(stdoutstring)
    if(match):
        isit = match.group(1)
        itis = 0
        if(isit == "IS"):
            itis = 1
        if(isit == "IS NOT"):
            itis = 0
    else:
        print "Could not match cadcWMD return value"
        itis = 0

    return (itis, stdoutstring)
