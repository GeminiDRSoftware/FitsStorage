FROM centos:centos8
LABEL maintainer="ooberdorf@gemini.edu"

# ENABLE EPEL
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
                   git
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
    pip3 install boto3 && \
    pip3 install pytest

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

# COPY STUFFS
ENV PYTHONPATH /opt/FitsStorage:/opt/DRAGONS
RUN cd /opt && git clone --branch master https://github.com/GeminiDRSoftware/DRAGONS.git
COPY . /opt/FitsStorage
WORKDIR /opt/FitsStorage
COPY fitsverify /opt/fitsverify

ENV PYTEST_SERVER archive
ENV FITS_DB_SERVER fitsdata:fitsdata@postgres-fitsdata
ENV PGPASSWORD fitsdata

CMD ["bash"]
