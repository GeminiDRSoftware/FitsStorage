FROM fitsservices:2022-1
LABEL maintainer="oliver.oberdorf@noirlab.edu"

# Note, we extend from fitsstorageservices just so we can have the static folder to serve css/etc from
# for the FITS Services.

# INSTALL HTTPD AND POSTGRESQL
RUN yum -y install --nogpgcheck httpd \
                   httpd-devel \
                   mod_ssl \
                   platform-python-devel \
                   nginx

# SETUP PYTHON PACKAGES - MOVE THESE TO REQUIREMENTS FILE
RUN dnf -y install redhat-rpm-config
#RUN pip3 install mod_wsgi==4.6.5
RUN pip3 install uwsgi

# SETUP UWSGI CONFIG
RUN mkdir /opt/uwsgi
COPY FitsStorage/otherfiles/uwsgi.ini /opt/uwsgi/fitsservice.ini

# SETUP UWSGI SERVICE
COPY FitsStorage/otherfiles/etc_systemd_system_fits-uwsgi.service /etc/systemd/system/fits-uwsgi.service

# SETUP NGINX
#COPY FitsStorage/otherfiles/nginx-fits /etc/nginx/sites-available/fits
#RUN ln -s /etc/nginx/sites-evailable/fits /etc/nginx/sites-enabled
COPY FitsStorage/otherfiles/nginx-fits /etc/nginx/nginx.conf

# PUT APACHE LOGS TO DOCKER OUTPUT
# need the modwsgi-default folder here so we can link out the logs
#RUN mkdir /opt/modwsgi-default
#RUN ln -sf /dev/stdout /var/log/httpd/access_log && \
#    ln -sf /dev/stderr /var/log/httpd/error_log && \
#    ln -sf /dev/stdout /opt/modwsgi-default/access_log && \
#    ln -sf /dev/stderr /opt/modwsgi-default/error_log && \
#    ln -sf /dev/stdout /var/log/messages

# PUT NGINX LOGS TO DOCKER OUTPUT
RUN ln -sf /dev/stdout /var/log/nginx/access.log && \
    ln -sf /dev/stderr /var/log/nginx/error.log && \
    ln -sf /dev/stdout /var/log/messages

# SETUP WEB SERVER
#RUN mod_wsgi-express setup-server --server-root /opt/modwsgi-default --port 80 --user apache --group apache --access-log --url-alias /static /opt/FitsStorage/htmldocroot --url-alias /favicon.ico /opt/FitsStorage/htmldocroot/favicon.ico --url-alias /robots.txt /opt/FitsStorage/htmldocroot/robots.txt --python-path /opt/FitsStorage --python-path /opt/DRAGONS --python-path /opt/GeminiCalMgr --python-path /opt/FitsStorageDB --proxy-mount-point /services/rest http://127.0.0.1:8090/services/rest  /opt/FitsStorage/fits_storage/wsgihandler.py
#RUN chown -R apache:apache /opt/modwsgi-default

# SETUP HTTPD SERVICE
#COPY FitsStorage/otherfiles/etc_systemd_system_fits-httpd.service /etc/systemd/system/fits-httpd.service
#COPY FitsStorage/otherfiles/etc-sysconfig-httpd /etc/sysconfig/httpd

# write a program to do the patch? could also do this by just copying in a fixed config file that we maintain, probably better
#COPY FitsStorage/otherfiles/httpd-patched-centos8.conf /opt/modwsgi-default/httpd.conf

#RUN   sed -i "s|#ProxyPass /services/rest http://127.0.0.1:8090/services/rest|ProxyPass /services/rest http://fitsservices:8090/services/rest |g" /opt/modwsgi-default/httpd.conf
#RUN   sed -i "s|#ProxyPassReverse /services/rest http://127.0.0.1:8090/services/rest|ProxyPassReverse /services/rest http://fitsservices:8090/services/rest |g" /opt/modwsgi-default/httpd.conf
#RUN   sed -i "s|#ProxyPass /services/calibrations http://127.0.0.1:8090/services/calibrations|ProxyPass /services/calibrations http://fitsservices:8090/services/calibrations |g" /opt/modwsgi-default/httpd.conf
#RUN   sed -i "s|#ProxyPassReverse /services/calibrations http://127.0.0.1:8090/services/calibrations|ProxyPassReverse /services/calibrations http://fitsservices:8090/services/calibrations |g" /opt/modwsgi-default/httpd.conf

EXPOSE 80
EXPOSE 443

#CMD ["/opt/modwsgi-default/apachectl", "-D", "FOREGROUND"]
CMD ["nginx", "-g", "daemon off;"]
#CMD ["uwsgi", "--http", "0.0.0.0:80", "--module", "fits_storage.wsgihandler"]

#ENTRYPOINT ["bash"]
