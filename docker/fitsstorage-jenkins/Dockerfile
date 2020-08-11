FROM centos:centos8.1.1911
# FROM gemini/epelhack:jenkins
LABEL maintainer="ooberdorf@gemini.edu"

# ENABLE EPEL
RUN yum -y install yum-utils
COPY spacewalk-epel-20200619.repo /tmp/spacewalk-epel-20200619.repo
RUN yum-config-manager --add-repo /tmp/spacewalk-epel-20200619.repo
#RUN dnf -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm

# IT Workaround - we can't resolve this host within Gemini network
#RUN echo "140.211.169.196 mirrors.fedoraproject.org" >> /etc/hosts
RUN cat /etc/hosts

# see: https://stackoverflow.com/questions/59993633/yum-dnf-error-failed-to-download-metadata-for-repo
# Do on build
### RUN dnf clean all
### RUN rm -r /var/cache/dnf
### RUN dnf upgrade -y
### RUN dnf update -y

# INSTALL HTTPD AND POSTGRESQL
RUN yum -y install openssl \
                   postgresql \
                   postgresql-devel \
                   epel-release \
                   gcc \
                   gcc-c++ \
                   gcc-gfortran \
                   cfitsio-devel \
                   git \
                   libjpeg-devel \
                   python3-pillow \
                   redhat-rpm-config \
                   python3-devel

##RUN yum -y --nogpgcheck install python3-pip
# WOW, Docker sucks.  You can't modify the hosts file except like this, just in time to run a command.  Because magical.  Or, magically bad.
# IT Workaround - we can't resolve this host within Gemini network
RUN echo "140.211.169.196 mirrors.fedoraproject.org" >> /etc/hosts && yum -y --nogpgcheck install python3-pip

# Fix Centos 8 broken Locale
RUN yum -y --nogpgcheck install glibc-langpack-en glibc-common
#RUN localedef --no-archive -i en_US -f UTF-8 en_US.UTF-8

# SETUP PYTHON PACKAGES - MOVE THESE TO REQUIREMENTS FILE
#RUN pip3 install --upgrade pip
RUN pip3 install psycopg2-binary && \
    pip3 install sqlalchemy && \
    pip3 install pyyaml && \
    pip3 install jinja2 && \
    pip3 install pyfits && \
    pip3 install dateutils && \
    pip3 install requests && \
    pip3 install matplotlib && \
    pip3 install scipy && \
    pip3 install pandas && \
    pip3 install astropy && \
    pip3 install future && \
    pip3 install psutil && \
    pip3 install boto3 && \
    pip3 install pytest && \
    pip3 install coverage && \
    pip3 install mysql-connector && \
    pip3 install simplejson && \
    pip3 install email

# Needed for imexam, which is needed for DRAGONS
RUN pip3 install photutils

RUN yum -y install make
RUN pip3 install imexam

# DRAGONS now also needs specutils
RUN pip3 install specutils


# CREATE FITSDATA USER AND GROUP
RUN /usr/sbin/groupadd -g 5179 fitsdata
RUN /usr/sbin/useradd -c 'FITS data' -u 5179 -g 5179 fitsdata

RUN echo "geminidata:x:502:fitsdata" >> /etc/group

# DATA FOLDERS
RUN mkdir -p /data/logs && \
    chown fitsdata /data/logs && \
    chmod oug+rwx /data/logs && \
    mkdir -p /data/backups && \
    chown fitsdata /data/backups && \
    mkdir -p /data/upload_staging && \
    chown fitsdata /data/upload_staging && \
    chmod oug+rwx /data/upload_staging && \
    mkdir -p /data/z_staging && \
    chown fitsdata /data/z_staging && \
    mkdir -p /data/s3_staging && \
    chown fitsdata /data/s3_staging && \
    chmod oug+rwx /data/s3_staging

# BECAUSE SPECUTILS (SIGH)
RUN mkdir -p /usr/share/httpd/.specutils && \
    chmod oug+rwx /usr/share/httpd/.specutils

# COPY STUFFS
ENV PYTHONPATH /opt/FitsStorage:/opt/DRAGONS
RUN cd /opt && git clone --branch master https://github.com/GeminiDRSoftware/DRAGONS.git
COPY . /opt/FitsStorage
WORKDIR /opt/FitsStorage
COPY fitsverify /opt/fitsverify

# CYTHON STEP FOR DRAGONS
RUN pip3 install cython
RUN cd /opt/DRAGONS/gempy/library/ && cythonize -i cyclip.pyx

# Calibs code
RUN cd /opt/FitsStorage/local_calibs && python3 setup.py install

# For specutils, wow this is dumb
RUN mkdir -p /.specutils && \
    chmod oug+rwx /.specutils

# Setup env
ENV PYTEST_SERVER archive
ENV FITS_DB_SERVER fitsdata:fitsdata@postgres-fitsdata
ENV PGPASSWORD fitsdata

CMD ["bash"]
