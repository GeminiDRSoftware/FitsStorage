FROM rockylinux/rockylinux:8
LABEL maintainer="oliver.oberdorf@noirlab.edu"

# ENABLE EPEL
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
                   python38-devel \
                   bzip2 \
                   zlib \
                   zlib-devel

RUN yum -y --nogpgcheck install python38-pip

# Fix Centos 8 broken Locale
RUN yum -y --nogpgcheck install glibc-langpack-en glibc-common

# SETUP PYTHON PACKAGES - MOVE THESE TO REQUIREMENTS FILE
# DRAGONS now also needs specutils
#RUN pip3 install gwcs==0.16.1
RUN pip3 install gwcs
#RUN pip3 install asdf==2.7.4
RUN pip3 install asdf
#RUN pip3 install specutils==1.2
RUN pip3 install specutils
RUN pip3 install psycopg2-binary && \
    pip3 install sqlalchemy && \
    pip3 install pyyaml && \
    pip3 install jinja2==3.0.2 && \
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
    pip3 install packaging && \
    pip3 install smart-open

# Needed for imexam, which is needed for DRAGONS
RUN pip3 install photutils

RUN yum -y install make

# RUN pip3 install git+https://github.com/spacetelescope/imexam.git
#RUN pip3 install imexam

# DRAGONS now also needs specutils
#RUN pip3 install gwcs==0.16.1
#RUN pip3 install asdf==2.7.4
#RUN pip3 install specutils==1.2

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
ENV PYTHONPATH /opt/FitsStorage:/opt/DRAGONS:/opt/GeminiCalMgr:/opt/FitsStorageDB
RUN cd /opt && git clone --branch master https://github.com/GeminiDRSoftware/DRAGONS.git

# TODO This will require some sort of ssh key setup, maybe not worth it
#RUN cd /opt && git clone --branch master git@gitlab.gemini.edu:DRSoftware/FitsStorageConfig.git
#RUN cd /opt && git clone --branch master git@gitlab.gemini.edu:DRSoftware/FitsStorageDB.git
#RUN cd /opt && git clone --branch master git@gitlab.gemini.edu:DRSoftware/GeminiCalMgr.git

COPY FitsStorageDB /opt/FitsStorageDB
COPY GeminiCalMgr /opt/GeminiCalMgr

RUN cd /opt/FitsStorageDB && python3 setup.py install
RUN cd /opt/GeminiCalMgr && python3 setup.py install
COPY FitsStorage /opt/FitsStorage
WORKDIR /opt/FitsStorage

RUN yum -y install libnsl
RUN ln -s /usr/lib64/libnsl.so.2 /usr/lib64/libnsl.so
COPY FitsStorage/fitsverify /opt/fitsverify
RUN cd /opt/fitsverify && gcc -o fitsverify ftverify.c fvrf_data.c fvrf_file.c fvrf_head.c \
    fvrf_key.c fvrf_misc.c -DSTANDALONE -L. -L/usr/lib64 -lcfitsio -lm -lnsl \
    -I /usr/include/cfitsio -L/usr/lib64/

COPY FitsStorage/docker/wait-for-postgres.sh /
RUN chmod a+x /wait-for-postgres.sh

# CYTHON STEP FOR DRAGONS
RUN pip3 install cython
RUN cd /opt/DRAGONS/gempy/library/ && cythonize -i cyclip.pyx

ENV PYTEST_SERVER archive
ENV FITS_DB_SERVER fitsdata:fitsdata@postgres-fitsdata
ENV PGPASSWORD fitsdata

RUN mkdir -p /usr/share/httpd/.specutils && \
    chmod oug+rwx /usr/share/httpd/.specutils && \
    chown fitsdata /usr/share/httpd/.specutils

CMD ["/bin/sh", "-c", "/bin/bash"]
