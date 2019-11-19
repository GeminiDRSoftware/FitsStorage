# FITSStorage

FITSStorage is used as a web application and set of background jobs both for the public facing `archive` website and
for the internal web portals for Gemini North and Gemini South.  It includes the code for ingesting new datafiles,
generating previews, and for feeding datafiles upstream from the individual sites to the main archive website.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Dev Options

There are multiple ways to deploy and run the FitsStorage website.  I am refactoring these instructions for
clarity by breaking out each option into a dedicated document.  While I am working on that, I will leave the
information below as it is still useful.

[OSX Dev](docs/OSX.md)
[Docker Dev](docs/DockerDev.md)

### Prerequisites

These instructions cover setting the project up to run for development on OSX.

You will need a copy of python3.  Anaconda is an easy way to get setup on OSX.  You will also need a PostgreSQL 
database.  For OSX, I like Postgres.App as it runs like a normal desktop application and doesn't clutter your 
system with new services.

 * https://anaconda.com/
 * https://postgresapp.com/

You can also install postres within a docker container or as a service.

I like to create a custom Anaconda environment to install python packages into.  This keeps my work in FitsStorage
independent of any other projects that I have going on.

```
conda create -n myenv python=3.6
conda activate myenv
```

There is a `requirements.txt` and a `requirements-test.txt` file that list the python requirements to run and test,
respectively, the project.  I generally just install all of these with pip3:

```
pip3 install requirements.txt
pip3 install requirements-test.txt
```

The project also uses the DRAGONS package developed by SUSD for Gemini.  I prefer to link in this dependency from a
full checkout of their codebase.

```
cd ~
git clone https://github.com/GeminiDRSoftware/DRAGONS.git
cd DRAGONS
pip3 install -e .
```

### Environment

Various features in the website can be configured via environment variables.  This allows you to, for instance,
change where FITS files are found on your Mac when running in PyCharm vs where it lives on the deployed CentOS servers.
Here are the environment variables you will likely want to set, tune according to your setup.

```shell 
export FITS_LOG_DIR=/Users/ooberdorf/logs/
export STORAGE_ROOT=/Users/ooberdorf/data/
export FITSVERIFY_BIN=/Users/ooberdorf/fitsverify/fitsverify
export VALIDATION_DEF_PATH=/Users/ooberdorf/dev-oly-tests-centos8/docs/dataDefinition/
export TEST_IMAGE_PATH=/Users/ooberdorf/data/
export TEST_IMAGE_CACHE=/Users/ooberdorf/tmp/cache/
export FITS_AUX_DATADIR=/Users/ooberdorf/dev-oly-tests-centos8/data/
export PYTHONPATH=~/DRAGONS:~/FitsStorage
```

If these environment variables are not found, the code falls back to defaults that are appropriate for the deployed
operational server.

### Installing

The install is managed by running an Ansible play.  This play is wrapped in a convenient shell script called
`archive_install.sh` in the `ansible` folder.  The play relies on you having a proper secrets setup to handle ssh
logins to the remote host, `hbffits-lv1.hi.gemini.edu`.

In the `ansible/playbooks` folder, you need to create a `secret` file.  Do this like so:

```
ansible-vault create secret
```

This will allow you to save a protected file that holds your ssh login password.  The file will look like this:

```
ansible_sudo_pass: mysudopassword
```

But now that file is also, in turn, password protected.  You can work around this with a "vault".  Create a file called
`vault.txt` in the `ansible/` folder and make the contents your password for the `secret` file created above.

Now you should have added two files.  Note that we *do not* add these to the repo.  This is a convoluted setup, but it
is what works.

```
ansible/playooks/secret
ansible/vault.txt
```

Now that this is done, assuming you have sudo permission to root on `hbffits-lv1`, you can run the ansible play.
To install, simply:

```
cd ansible
bash ./archive_install.sh
```

Once the install finishes, you should be able to browse the deployed site at:

https://hbffits-lv1.hi.gemini.edu/searchform/

## Running the tests

All of the tests are written with `pytest` and live in the `tests` folder.  You can run them with

`pytest tests`

However, any tests that rely on the database or the website have been tagged as slow.  If you want to run these tests
as well, you can add the `--runslow` argument.

`pytest --runslow tests`

Note that the tests will decide what webserver and database to connect to based on some environment variables.

```shell 
ENV PYTEST_SERVER archive
ENV FITS_DB_SERVER fitsdata:fitsdata@postgres-fitsdata
```

## Docker

Additional work has gone into making FitsStorage work with Docker containers.  This largely involves the use of three
separate containers: one for the PostgreSQL database, one for the WSGI website, and one for running tools such as the
datafile ingest.

All of the docker infrastructure lives under the `docker/` subfolder.  There are is a set of folders for each image
and a `script` folder with shell scripts for building images and creating containers.  The primary scripts you want to
look at are

Script to create the images:

```shell 
buildfitsstorageutils.sh
buildarchive.sh
```

Script to create the containers:

```shell 
postgres.sh
api.sh
archive.sh
fitsstorageutils.sh
```

The other Dockerfiles are for a CentOS 7 version of the archive and for some Jenkins CI/CD support.  
`archive.sh` exposes ports 80 and 443 into the webserver by default and names the container `archive`.
You can alter these values with command line arguments like this (you can drop extra arguments if you don't need
all 4):

`archive.sh container_name http_port https_port database`  

## Built With

* [DRAGONS](https://github.com/GeminiDRSoftware/DRAGONS) - The utilities for working with Gemini FITS files.

## Versioning

We use [CalVer](https://calver.org/) for versioning.  Versions are expressed as `YYYY-V` where `V` is the release number
within that year.  For the versions available, see the 
[branches on this repository](https://gitlab.gemini.edu/DRSoftware/FitsStorage/branches?utf8=%E2%9C%93&search=20) 
that begin with a year. 

## Authors

* **Paul Hirst** - *Initial work*
* **Ricardo Cardenes** - *Initial work*
* **Ken Anderson** - *Initial work*
* **Oliver Oberdorf** - *2020 Python 3 based release and CI/CD*

## License

This project is licensed under the BSD License - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* DRAGONS for providing simple access to complex datafiles
