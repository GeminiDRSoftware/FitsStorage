FROM centos:centos8
LABEL maintainer="ooberdorf@gemini.edu"

# ENABLE EPEL
# Trying this hack
#   https://www.edureka.co/community/72932/error-failed-to-download-metadata-for-repo-epel-modular
RUN rm -r /var/cache/dnf
RUN dnf -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm

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

RUN yum -y --nogpgcheck install python3-pip

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
    pip3 install mysql-connector && \
    pip3 install simplejson && \
    pip3 install email

# Needed for imexam, which is needed for DRAGONS
RUN pip3 install photutils

RUN yum -y install make
# RUN cd /opt && git clone https://github.com/ericmandel/xpa.git
# RUN cd /opt/xpa && ./configure && make && make install
# RUN yum -y install tcl-8.5.13-4.el7.x86_64
# RUN yum -y install http://download-ib01.fedoraproject.org/pub/epel/7/x86_64/Packages/x/xpa-libs-2.1.18-1.el7.1.x86_64.rpm
# RUN yum -y install http://download-ib01.fedoraproject.org/pub/epel/7/x86_64/Packages/x/xpa-2.1.18-1.el7.1.x86_64.rpm
# RUN yum -y install http://download-ib01.fedoraproject.org/pub/epel/7/x86_64/Packages/x/xpa-devel-2.1.18-1.el7.1.x86_64.rpm

# Need the latest imexam for DRAGONS    

# RUN cd /opt && git clone --recursive https://github.com/spacetelescope/imexam.git
# RUN cd /opt/imexam && git submodule update --init -- cextern/xpa
# RUN cd /opt/imexam/cextern/xpa && ./configure && make && make install
# RUN cd /opt/imexam && ls -l cextern/xpa/conf.h
# RUN cd /opt/imexam && python3 setup.py install

# RUN pip3 install git+https://github.com/spacetelescope/imexam.git
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
    mkdir -p /data/backups && \
    chown fitsdata /data/backups && \
    mkdir -p /data/upload_staging && \
    chown fitsdata /data/upload_staging && \
    chmod oug+rwx /data/upload_staging && \
    mkdir -p /data/z_staging && \
    chown fitsdata /data/z_staging && \
    mkdir -p /data/s3_staging && \
    chown fitsdata /data/s3_staging && \
    chmod oug+rw /data/s3_staging && \
    chmod oug+rw /data/logs

# COPY STUFFS
ENV PYTHONPATH /opt/FitsStorage:/opt/DRAGONS
#RUN cd /opt && git clone --branch vrefac/processed_standard https://github.com/GeminiDRSoftware/DRAGONS.git
RUN cd /opt && git clone --branch master https://github.com/GeminiDRSoftware/DRAGONS.git
#RUN cd /opt && git clone --branch refac/populate_provenance_data https://github.com/GeminiDRSoftware/DRAGONS.git
#RUN cd /opt && git clone --branch refac/imexam_workaround https://github.com/olyoberdorf/DRAGONS.git
COPY . /opt/FitsStorage
WORKDIR /opt/FitsStorage
COPY fitsverify /opt/fitsverify

# CYTHON STEP FOR DRAGONS
RUN pip3 install cython
RUN cd /opt/DRAGONS/gempy/library/ && cythonize -i cyclip.pyx

# Calibs code
RUN cd /opt/FitsStorage/local_calibs && python3 setup.py install

ENV PYTEST_SERVER archive
ENV FITS_DB_SERVER fitsdata:fitsdata@postgres-fitsdata
ENV PGPASSWORD fitsdata

RUN mkdir -p /usr/share/httpd/.specutils && \
    chmod oug+rwx /usr/share/httpd/.specutils && \
    chown fitsdata /usr/share/httpd/.specutils

CMD ["/bin/sh", "-c", "/bin/bash"]
