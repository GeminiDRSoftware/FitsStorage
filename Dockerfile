FROM centos:centos7
LABEL maintainer="ooberdorf@gemini.edu"

# INSTALL HTTPD AND POSTGRESQL
RUN yum -y install httpd \
                   httpd-devel \
                   mod_ssl \
                   openssl \
                   postgresql \
                   postgresql-devel \
                   epel-release \
                   gcc \
                   gcc-c++ \
                   python-devel \
                   gcc-gfortran \
                   cfitsio-devel \
                   git
RUN yum -y install python-pip

# SETUP PYTHON PACKAGES - MOVE THESE TO REQUIREMENTS FILE
RUN pip install --upgrade pip
RUN pip install mod_wsgi==4.6.5 && \
    pip install psycopg2 && \
    pip install sqlalchemy && \
    pip install pyyaml && \
    pip install jinja2 && \
    pip install pyfits && \
    pip install dateutils && \
    pip install requests && \
    pip install matplotlib && \
    pip install scipy && \
    pip install pandas && \
    pip install astropy && \
    pip install future && \
    pip install boto3

# PUT APACHE LOGS TO DOCKER OUTPUT
# need the modwsgi-default folder here so we can link out the logs
RUN mkdir /opt/modwsgi-default
RUN ln -sf /dev/stdout /var/log/httpd/access_log && \
    ln -sf /dev/stderr /var/log/httpd/error_log && \
    ln -sf /dev/stdout /opt/modwsgi-default/access_log && \
    ln -sf /dev/stderr /opt/modwsgi-default/error_log

# CREATE FITSDATA USER AND GROUP
RUN /usr/sbin/groupadd -g 5179 fitsdata
RUN /usr/sbin/useradd -c 'FITS data' -u 5179 -g 5179 fitsdata

RUN echo "geminidata:x:502:fitsdata" >> /etc/group

# DATA FOLDERS
RUN mkdir -p /data/logs && \
    chown fitsdata /data/logs && \
    mkdir -p /data/backups && \
    chown fitsdata /data/backups && \
    mkdir -p /data/upload_staging && \
    chown fitsdata /data/upload_staging && \
    chmod oug+rwx /data/upload_staging && \
    mkdir -p /data/z_staging && \
    chown fitsdata /data/z_staging && \
    mkdir -p /data/s3_staging && \
    chown fitsdata /data/s3_staging && \
    chmod oug+rw /data/s3_staging

# COPY STUFFS
RUN cd /opt && git clone https://github.com/GeminiDRSoftware/DRAGONS.git
COPY . /opt/FitsStorage
WORKDIR /opt/FitsStorage

# MAP FITSTORAGE AND DRAGONS TO HOST
#VOLUME /opt/FitsStorage .
#VOLUME /opt/DRAGONS ../DRAGONS

# get fitsverify from /opt on a host

# SETUP WEB SERVER
#COPY install_script.sh .
#RUN bash ./install_script.sh
RUN mod_wsgi-express setup-server --server-root /opt/modwsgi-default --port 80 --user apache --group apache --access-log --url-alias /static /opt/FitsStorage/htmldocroot --url-alias /favicon.ico /opt/FitsStorage/htmldocroot/favicon.ico --url-alias /robots.txt /opt/FitsStorage/htmldocroot/robots.txt --python-path /opt/FitsStorage --python-path /opt/DRAGONS  /opt/FitsStorage/fits_storage/wsgihandler.py
RUN chown -R apache:apache /opt/modwsgi-default

# SETUP HTTPD SERVICE
COPY otherfiles/etc_systemd_system_fits-httpd.service /etc/systemd/system/fits-httpd.service
COPY otherfiles/etc-sysconfig-httpd /etc/sysconfig/httpd

# write a program to do the patch? could also do this by just copying in a fixed config file that we maintain, probably better
COPY httpd-patched.conf /opt/modwsgi-default/httpd.conf

# POSTGRESQL
# pg data dir
#RUN mkdir -p /data/pgsql_data
#RUN chown postgres:postgres /data/pgsql_data
#RUN cp /lib/systemd/system/postgresql.service /etc/systemd/system

#RUN sed 's,Environment=PGDATA=/var/lib/pgsql/data,Environment=PGDATA=/data/pgsql_data,g' < /etc/systemd/system/postgresql.service > /tmp/postgresql.service.patched
#RUN cp /tmp/postgresql.service.patched /etc/systemd/system/postgresql.service

#RUN systemctl daemon-reload
#RUN postgresql-setup initdb
#RUN systemctl start postgresql.service
#RUN su - postgres -c /usr/bin/createuser --no-superuser --no-createrole --createdb fitsdata
#RUN su - postgres -c /usr/bin/createuser --no-superuser --no-createrole --no-createdb apache

EXPOSE 80
EXPOSE 443

ENTRYPOINT ["/opt/modwsgi-default/apachectl", "-D", "FOREGROUND"]
#ENTRYPOINT ["bash"]
