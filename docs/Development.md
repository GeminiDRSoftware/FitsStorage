## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and 
testing purposes. See [README](../README.md) for notes on how to deploy the project on a live system.

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

 * [OSX Dev](docs/OSX.md)
 * [Docker Dev](docs/DockerDev.md)

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
