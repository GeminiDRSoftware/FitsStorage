# FITSStorage

FITSStorage is used as a web application and set of background jobs both for the public facing `archive` website and
for the internal web portals for Gemini North and Gemini South.  It includes the code for ingesting new datafiles,
generating previews, and for feeding datafiles upstream from the individual sites to the main archive website.

## Quick Deploys

These links are if you just want to quickly deploy to a target environment

 * [Build and deploy Dev](http://ooberdorf:11a3f74b7cffa0dd06ceeca74e9773a904@hbffits-lv3.hi.gemini.edu:8080/job/fitsstorage/parambuild?token=qpZIKjlU3xSlM9JA3wwFjv8CJsu5lhYM&deploy_target=dev&cause=Manually%20triggered%20from%20url)
 * [Build and deploy QAP](http://ooberdorf:11a3f74b7cffa0dd06ceeca74e9773a904@hbffits-lv3.hi.gemini.edu:8080/job/fitsstorage/parambuild?token=qpZIKjlU3xSlM9JA3wwFjv8CJsu5lhYM&deploy_target=dev-qap&cause=Manually%20triggered%20from%20url)

## Development

For developing the FITSStorage code, see the notes on development here:

 * [Development Notes](docs/Development.md)
 
### Installing

The install is managed by running an Ansible play.  This play is wrapped in a convenient shell script called
`archive_install_internal.sh` in the `ansible` folder.  There is also an `archive_install_aws.sh` for installs to
  the AWS host (be careful!).  The play relies on you having a proper secrets setup to handle ssh
logins to the remote host.  You also need to have sudo permission to root on the target host.

To install, simply:

```
cd ansible
bash ./archive_install_internal.sh -i dev
```

Once the install finishes, you should be able to browse the deployed site at (modify as appropriate):

https://hbffits-lv4.hi.gemini.edu/searchform/

| *Filename*        | *Host*             | *Notes* |
|-------------------|--------------------|-------|
| dev               | hbffits-lv4        | This is the primary development host.  Normally, deploys are automatic from Jenkins. |
| dev-cl            | cpofits-lv2        | A development host available in Chile.  Mainly for testing skycam data. |
| dev-qap           | something          | A development host for integrating with DRAGONS/QAP |
| dev-aws           | N/A                | Currently not provisioned.  For deploying to an AWS dev host near public release |
| cpo               | cpofits-lv3        | The Chile operational FITSStorage host |
| mko               | mkofits-lv3        | The Hawaii operational FITSStorage host |
| aws *CAUTION*     | archive.gemini.edu | The public GOA host |


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

## Database Notes

Database notes are captured here:

* [Database Notes](docs/Database.md)

## AWS

Documents related to our AWS setup

* [Setup](docs/AWSSetup.md)
* [Emergency Procedures](docs/AWSEmergencyProcedures.md)
* [Spot Instance Notes](docs/AWSSpotInstanceNotes.md)
* [Promotion Checklist](docs/ArchivePromotionChecklist.md)
* [Database Upgrade Notes](docs/ArchiveDatabaseUpgradeNotes.md)

## Email Server

We use a fairly simple email server relay configuration on `postfix`.  More 
information is documented here:

* [Email Relay Setup](docs/GeminiMailSetup.md)

## Let's Encrypt SSL Setup

We use Let's Encrypt as our SSL certificate provider.  Their tools renew the cert every
90 days.

* [SSL Install](docs/ssl_install.md)

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
