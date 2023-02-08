# ORCID Support

ORCID Support for the Archive allows users to log in using their ORCID
credentials.  These can either be attached to an existing account or be
the basis for a brand new account.

## Enabling ORCID For Server

ORCID support may be enabled or disabled individually for FITS Store servers.
This is controlled in `fits_storage_config` in the `ORCID_ENABLED` setting.
If ORCID is disabled, attempts to login via ORCID will fail with a message to
that effect.  Also, ORCID related options will not be visible in the Login 
drop down in the website top bar.

## Create ORCID Backed Account

If a user is not logged in, there is an additional option in the Login menu
allowing them to login with their ORCID account.  When they do this, the 
web server will go through the login/handshake process with ORCID.  The 
users ORCID session will then be passed back to us.  At that point, we have
their ORCID ID and full name.

If we already know this ORCID ID, we will just log the user in normally.  
If this is a new ORCID ID to us, we create a new account for that ORCID ID
and add their full name.  This new account has no `username` and cannot
login via password.

## Associate ORCID ID To Password Account

If a user is logged in normally, they will see a different option in the Login
menu to associate their ORCID account.  If they choose this option, they go
through the same ORCID handoff/callback as before.  However, now we see that 
they are already logged in and we associate their ORCID ID with their current
account.  Now that account may login either with a password or via ORCID.  
Either way we will setup a session for that user with all the permissions they
would normally have.

## Setting Up Demo ORCID Account

ORCID has a demonstration sandbox service we use for development.  You can 
create a test account on this service to make use of the FitsStore's ORCID
features.

### Step 1, Create Email

ORCID sandbox requires an email account from `mailinator.com`.  To create an
account on that service, browse to `https://www.mailinator.com/v3/#/#inboxpane`.
There, in the top right, enter an email name and hit 'Go'.  There is no password and
this email account can be viewed by anyone who enters that name.  ORCID's sandbox 
will not support any other email domains.

### Step 2, Create ORCID Account

Go to the ORCID Sandbox here: `https://sandbox.orcid.org/signin` and choose 
Register.  Enter your name, the mailinator email, and a password.  Later, you 
will need the email address and password to login to ORCID.

### Step 3, Go To FitsStore Dev Server

Browse to `http://hbffits-lv4.hi.gemini.edu`.  In the upper right top bar area, 
there is a new menu option to Login with ORCID.  Select that option.  There is
also an "add ORCID ID" option if you are already logged in.

### Step 4, Login to ORCID

You will be routed to the ORCID Sandbox login page.  After logging in, ORCID will
return you to the `hbffits-lv4` search page.  The FITS server will get your
 identity from the ORCID server.  Their server is very slow for some reason, so just
 be patient.  I plan to add some sort of loading indicator.  After the server
 gets the ORCID account information, you will be logged in and your
ORCID account will appear in the upper right where username would be.
