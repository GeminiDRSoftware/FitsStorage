# FITSStorage

FITSStorage is used as a web application and set of background jobs both for the public facing `archive` website and
for the internal web portals for Gemini North and Gemini South.  It includes the code for ingesting new datafiles,
generating previews, and for feeding datafiles upstream from the individual sites to the main archive website.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Ultra-Quick-Start

To get things up and running as fast as possible, I recommend the "lite" Docker Compose recipe.  To do this, there
are just a few steps.  I'll assume you installed docker and you have a fresh chekout of this repo.

1. Build the images

    ```
    <cd to top-level of FitsStorage codebase>
    bash ./docker/scripts/buildfitsstorageutils.sh
    bash ./docker/scripts/buildarchive.sh
    ```

2. Make a dataflow directory

    ```
    mkdir ~/dataflow/
    cp <somefiles> ~/dataflow/
    ```

3. Run the mini cluster

    ```
    docker-compose -f docker-compose-lite.yml up
    ```

4. Shut down

    ```
    <Ctrl-C in the terminal>
    docker-compose -f docker-compose-lite.yml down
    ```

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
`archive_install_internal.sh` in the `ansible` folder.  The play relies on you having a proper secrets setup to handle ssh
logins to the remote host.


Now that this is done, assuming you have sudo permission to root on the target host, you can run the ansible play.
To install, simply:

```
cd ansible
bash ./archive_install.sh -i dev
```

Once the install finishes, you should be able to browse the deployed site at (modify as appropriate):

https://hbffits-lv1.hi.gemini.edu/searchform/

### Crontab

I have removed the crontab from the ansible play to avoid problems.  Adding or updating cron can be done manually.
Also note that historically we have had issues with the cron deploy.  Make sure the crontabs work as we have seen
the ansible user module create users that are broken on CentOS 8 for cronjobs.

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

## Configuration

Configuration of the FitsStorage hosts is done via a combination of environment variables,
an `/etc` config file, some host-specific overrides and the default values in `fits_storage_config.py`.
Information on how configuration works is here:

[Configuration](docs/Configuration.md)

## Docker

To develop with a cluster of servers or with a specific version of CentOS, we can use docker
containers to simulate that environment.  Long-term, I imagine we'll be running the real
servers in containers as well, so this is also foundation work for that.  Information for
using Docker to run the website and realted tools is documented here:

* [Docker](docs/Docker.md)

## Installation

Installing to servers is done using Ansible.  The ansible plays and inventories all live in the
`ansible` folder.  You can find more information about using the ansible deploys here:

* [Ansible](docs/Ansible.md)

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
