## Postfix Setup

This configuration covers relaying to the email gateway.  The host needs to be 
whitelisted - talk to Chris Stark about this.

### `/etc/postfix/main.cf`

This can be owner and group of `root`.  The full contents of the file are:

```
inet_protocols = ipv4
relayhost = [smtp.hi.gemini.edu]:25
```

And of course, substitute `cl` for Gemini South hosts.
