# SSL Cert Installation with Let's Encrypt

This document covers how to install the SSL certificates for the archive website using
Let's Encrypt and certbot.

## Install Certbot

```
sudo yum install -y epel-release
sudo yum install -y certbot python3-certbot-apache mod_ssl
```

## Obtain Certificate

```
sudo certbot certonly --webroot -w /opt/modwsgi-default/htdocs/ -d arcdev.gemini.edu
```