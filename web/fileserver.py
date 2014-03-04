from orm import sessionfactory

from fits_storage_config import odbkeypass, using_s3

from gemini_metadata_utils import gemini_fitsfilename

from orm.file import File
from orm.diskfile import DiskFile
from orm.header import Header
from orm.authentication import Authentication

# This will only work with apache
from mod_python import apache
from mod_python import Cookie
from mod_python import util

import time
import urllib
import re
import datetime

if(using_s3):
    from boto.s3.connection import S3Connection
    from fits_storage_config import aws_access_key, aws_secret_key, s3_bucket_name

def authcookie(req):
    """
    This is the funciton to authenticate the user and send them the cookie
    """
    # Process the form data first if there is any
    login_attempt = False
    valid_login = False
    formdata = util.FieldStorage(req)
    if(('program_id' in formdata.keys()) and ('program_key' in formdata.keys())):
        # OK, this is a login attempt
        login_attempt = True
        program_id = formdata['program_id'].value
        program_key = formdata['program_key'].value

        # Here is where we do the actual authentication
        if(program_key == odb_key(program_id)):
            valid_login = True

    req.content_type = "text/html"
    if(valid_login):
        # Cookie expires in 1 year
        Cookie.add_cookie(req, program_id, program_key, expires = time.time()+31536000)
    req.write("<html><head><title>FITS Storage Download Authentication</title></head><body>")
    req.write("<h1>FITS Storage Download Authentication</h1>")

    #req.write("<h3>debug</h3>")
    #req.write("<p>%s</p>" % str(formdata))
    #if(login_attempt):
        #req.write("<p>Login Attempt - Prog id: %s - Prog key: %s</p>" % (program_id, program_key))
    #req.write("<HR>")

    req.write("<H2>How this works</H2>")
    req.write("<P>When you authenticate by giving this page a valid program ID and program key (same as you use in the OT to fetch and store your program), this web server will send an HTTP cookie to your browser that will allow you to download data for that program from this web browser (or others that share its cookie file, for example sharing the same home directory) in the future without authenticating each time.</P>")
    req.write("<P>A cookie is a small piece of data that your web browser remembers and shows to the server when you request a file to tell the server that the browser is authorized to download data for a given program ID. You should not log into this page if you don't want users of this browser's cookie file to be able to download data for your program.</P>")

    if(valid_login):
        session = sessionfactory()
        try:
            # Does this program already exist in the auth table?
            query = session.query(Authentication).filter(Authentication.program_id == program_id)
            if(query.count() == 0):
                # No, then add it
                auth = Authentication()
                auth.program_id = program_id
                auth.program_key = program_key
                session.add(auth)
                session.commit()
            else:
                # Ensure the program key is the current one
                auth = query.one()
                if(auth.program_key != program_key):
                    auth.program_key = program_key
                    session.commit(auth)
        except IOError:
            pass
        finally:
            session.close()
         
        req.write("<h2>Authentication Suceeded</h2>")
        req.write("<p>Your browser is being sent the HTTP cookie with this page.</p>")
        req.write("<p>You can use the cookie in your own scripts, or from the command line, either by having your download tool reference this browsers cookie file, or by giving it the cookie manually.</p>")
        req.write("<p>The cookie's name:value is %s:%s</p>" % (program_id, program_key))

        req.write('<p>You probably want to go to <a href="/mydata/%s">your programs mydata page at /mydata/%s</a> now.</p>' % (program_id, program_id))

    else:
        if(login_attempt):
            req.write("<h2>Authentication Failed</h2>")
            req.write("<p>Please check you have the correct program id and program key</p>")

        req.write("<H2>Login</H2>")
        req.write('<FORM action="/authentication" method="POST">')
        req.write("<TABLE>")
        req.write('<TR><TD><LABEL for="program_id">Program ID</LABEL></TD><TD><INPUT type="text" size=32 name="program_id"</INPUT></TD></TR>')
        req.write('<TR><TD><LABEL for="program_key">Program Key</LABEL></TD><TD><INPUT type="text" size=8 name="program_key"</INPUT></TD></TR>')
        req.write("</TABLE>")
        req.write('<INPUT type="submit" value="Submit"></INPUT> <INPUT type="reset"></INPUT>')
        req.write('</FORM>')

    req.write("</body></html>")
    
    return apache.OK
    
html_cre = re.compile("Password for Program <i>(\S*)</i>: <b>(\S*)</b>")
def odb_key(program_id):
    """
    This function queries the ODB for the program key for a given program
    """

    url = "http://phase1.cl.gemini.edu/cgi-bin/gemini/progIdHash.pl?programId=%s&pass=%s" % (program_id, odbkeypass)
    u = urllib.urlopen(url)
    html = u.read()
    u.close()
    match = html_cre.search(html)
    key = None
    if(match):
        if(match.group(1) == program_id):
            key = match.group(2)

    return key
    
def fileserver(req, things):
    """
    This is the fileserver funciton. It handles authentication for serving the files too
    """

    # OK, first find the file they asked for in the database
    # tart up the filename if possible
    if(len(things) == 0):
        return apache.HTTP_NOT_FOUND
    filenamegiven = things.pop(0)
    filename = gemini_fitsfilename(filenamegiven)
    if(filename):
        pass
    else:
        filename = filenamegiven
    session = sessionfactory()
    try:
        query = session.query(File).filter(File.name == filename)
        if(query.count() == 0):
            return apache.HTTP_NOT_FOUND
        file = query.one()
        # OK, we should have the file record now.
        # Next, find the canonical diskfile for it
        query = session.query(DiskFile).filter(DiskFile.present == True).filter(DiskFile.file_id == file.id)
        diskfile = query.one()
        # And now find the header record...
        query = session.query(Header).filter(Header.diskfile_id == diskfile.id)
        header = query.one()

        # OK, now figure out if the data are public
        today = datetime.datetime.utcnow().date()
        canhaveit = False

        # Are we passed the release data?
        if((header.release) and (today >= header.release)):
            # Yes, the data are public
            canhaveit = True

        # Is the data a dayCal or a partnerCal or an acqCal?
        elif(header.observation_class in ['dayCal', 'partnerCal', 'acqCal']):
            # Yes, the data are public. These should have a release date too, except that
            # Cals from the pipeline processed directly off the DHS machine don't
            canhaveit = True

        else:
            # No, the data are not public. See if we got the magic cookie
            cookies = Cookie.get_cookies(req)
            if(cookies.has_key('gemini_fits_authorization')):
                auth = cookies['gemini_fits_authorization'].value
                if(auth == 'good_to_go'):
                    # OK, we got the magic cookie
                    canhaveit = True

        # Did we get a program ID authentication cooke?
        cookie_key = None
        # Is this program ID in the authentication table? If so, what's the key?
        program_key = None
        program_id = header.program_id
        authquery = session.query(Authentication).filter(Authentication.program_id == program_id)
        if(authquery.count() == 1):
            auth = authquery.one()
            program_key = auth.program_key
        cookies = Cookie.get_cookies(req)
        if(cookies.has_key(program_id)):
            cookie_key = cookies[program_id].value
        if((program_key is not None) and (program_key == cookie_key)):
            canhaveit = True

        if(canhaveit):
            # Send them the data
            req.content_type = 'application/fits'
            req.headers_out['Content-Disposition'] = 'attachment; filename="%s"' % filename
            if(using_s3):
                # S3 file server
                # For now, just serve what we have.
                # Need to implement gz and non gz requests somehow
                s3conn = S3Connection(aws_access_key, aws_secret_key)
                bucket = s3conn.get_bucket(s3_bucket_name)
                key = bucket.get_key(filename)
                req.set_content_length(diskfile.file_size)
                key.get_contents_to_file(req)
            else:
                # Serve from regular file
                if(diskfile.gzipped == True):
                    # Unzip it on the fly
                    req.set_content_length(diskfile.data_size)
                    gzfp = gzip.open(diskfile.fullpath(), 'rb')
                    try:
                        req.write(gzfp.read())
                    finally:
                        gzfp.close()
                else:
                    req.sendfile(diskfile.fullpath())

            return apache.OK
        else:
            # Refuse to send data
            return apache.HTTP_FORBIDDEN

    except IOError:
        pass
    finally:
        session.close()

def mydata(req, selection):
    """
    This is the "mydata" landing page for remote eavesdroppers and others 
    expecting to download data from this server in a similar context to being a PI
    """

    req.content_type = "text/html"

    if('program_id' not in selection):
        req.write('<html><head><title>FITS Storage MyData Page</title></head><body>')
        req.write('<h1>FITS Storage MyData Page</h1>')
        req.write('<p>You must supply a program ID to use the MyData pages</p>')
        req.write('</body></html>')
        return apache.OK

    program_id = selection['program_id']
    title = 'FITS Storage MyData Page - %s' % program_id
    req.write('<html><head><title>%s</title></head><body>' % title)
    req.write('<h1>%s</h1>' % title)

    # First see if they already authenticated for this project ID
    # Get a DB session
    session = sessionfactory()
    try:
        req.write('<h2>Authentication</h2>')
        query = session.query(Authentication).filter(Authentication.program_id == program_id)
        in_auth_table = (query.count()>0)
        if(not in_auth_table):
            req.write('<P>This program has not been authenticated for downloads from this fits server, please visit <a href="/authentication">the authentication page</a> and supply your phase 2 program key to authenticate with this server and receive a browser authorization cookie which will allow you to download data.</P>')

            req.write('<P style="color:red"><BIG><STRONG>You have not successfully authenticated on this browser. You will not be allowed to download data. Go to <a href="/authentication">the authentication page</a>!</STRONG></BIG></P>')

        else:
            # count is >0 so there must be an auth entry for this project id
            auth = query.one()
            program_key = auth.program_key
            # Did they send us a matching cookie?
            cookies = Cookie.get_cookies(req)
            cookie_key = None
            if(cookies.has_key(program_id)):
                cookie_key = cookies[program_id].value
            if((program_key is not None) and (program_key == cookie_key)):
                req.write('<P>This program has already authenticated for downloads from this server and your browser is supplying the authorization cookie. Authentication is all good, this server will allow you to download your data with this browser.</P>')
            else:
                req.write('<P>This program has authenticated for downloads from this server, but your browser is not supplying a valid authorization cookie. Please <a href="/authentaction">re-authenticate from this browser</a> if you wish to download data using this browser.</P>')
                req.write('<P style="color:red"><BIG><STRONG>You have not successfully authenticated on this browser. You will not be allowed to download data. Go to <a href="/authentication">the authentication page</a>!</STRONG></BIG></P>')

        req.write('<H2>Data summaries with download links</H2>')
        req.write('<P>Note that a [download] link will show against all files, however the server will only send you data for files for which you have access to - either calibration data (which is public) or data for a program you are authorized for (see above).</P>') 

        req.write('<UL>')
        req.write('<LI>All files from your program: <a href="/summary/%s/download">/summary/%s/download</a></LI>' % (program_id, program_id))
        req.write('<LI>All files from current UT date: <a href="/summary/today/download">/summary/today/download</a></LI>')
        req.write('<LI>All files from your program on current UT date: <a href="/summary/today/%s/download">/summary/today/%s/download</a></LI>' % (program_id, program_id))

        req.write('</UL>')
        req.write('</body></html>')

        return apache.OK


    except IOError:
        pass
    finally:
        session.close()
