# SSL Cert Installation with Let's Encrypt

This document covers how to install the SSL certificates for the archive website using
Let's Encrypt and certbot.  Ansible attempts to handle this during install.

## Install Certbot

Nginx:

```
sudo yum install -y certbot python3-certbot-nginx
```

Apache:

```
sudo yum install -y epel-release
sudo yum install -y certbot python3-certbot-apache mod_ssl
```

## Obtain Certificate

Nginx:

```
sudo certbot certonly --nginx -d archive.gemini.edu
```

Apache:

```
sudo certbot certonly --webroot -w /opt/modwsgi-default/htdocs/ -d arcdev.gemini.edu
```