# FITSStorage

FITSStorage is used as a web application and set of background jobs both for the public facing `archive` website and
for the internal web portals for Gemini North and Gemini South.  It includes the code for ingesting new datafiles,
generating previews, and for feeding datafiles upstream from the individual sites to the main archive website.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

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

### Installing

A step by step series of examples that tell you how to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

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
