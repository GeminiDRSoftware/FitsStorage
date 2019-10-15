FROM centos:centos7
LABEL maintainer="ooberdorf@gemini.edu"

# INSTALL HTTPD AND POSTGRESQL
RUN yum -y install openssl \
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
RUN pip install psycopg2 && \
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
ENV PYTHONPATH /opt/FitsStorage:/opt/DRAGONS
RUN cd /opt && git clone https://github.com/GeminiDRSoftware/DRAGONS.git
COPY . /opt/FitsStorage
WORKDIR /opt/FitsStorage
COPY fitsverify /opt/fitsverify

ENV PYTEST_SERVER archive

ENTRYPOINT ["bash"]
