## Docker Dev Setup

FitsStorage can be run via Docker containers.  This has the advantage of being closer to
what the system will be like in the deployed servers, with matching OS and layouts.  It
also makes it very easy to clean up and start from a known state.  Finally, this leverages
the same docker support we are using for doing CI/CD testing.

### Getting Started

To begin, you will want to install the latest Docker on your machine.  I am using a
MacBook, so my instructions will be geared to OSX but Linux and Windows should also 
work.

In addition to docker, you may want to install `sshfs`.  `sshfs` will allow you to
mount remote data folders onto your machine.  From there, you can map these folders
into the archive container where appropriate.  In other words, you could do this to
make it easier to restore a DB backup onto a fresh Postgres, or to be able to serve
file downloads from your dev archive without pulling down all the datafiles.
However, I would suggest getting your docker setup without it first and then come
back to it when you are ready.

### Folders

The containers built by our full compose file depend on a few folders.  It is
best if you create these folders now and toss some sample data into 
`onsite-sci-dataflow` for ingest later.  These folders are:

```
mkdir -p ~/testdata/archive-data-upload_staging
mkdir -p ~/testdata/archive-sci-dataflow
mkdir -p ~/testdata/onsite-data-upload_staging
mkdir -p ~/testdata/onsite-sci-dataflow
```

Copy your datafiles into `~/testdata/onsite-sci-dataflow`

You do not need these folders if you opt for the "lite" compose file.  That one simply 
requires a `~/dataflow/` folder with the data you want to use.

### Compose

There is now a very fast way to get a test cluster up complete with two webservers for
'onsite' and 'archive' (public).  You can just use `docker-compose`.  There is a `docker-compose.yml`
file in the top directory of the tree.  Just change to the top folder of your checkout and
run `docker-compose up`.  Instructions for running containers manually are listed below in case
you don't want to use docker-compose.

If you want a simplified cluster to just run a single FitsStore plus Postgres database,
then you can use `docker-compose-lite.yml`.  This just looks for a `~/dataflow/` directory
for files to import.  Make that folder and put the files you want in it.  Then run
`docker-compose -f docker-compose-lite.yml up` to start the webserver.  Now you can browse
to `http://localhost/searchform` to see the normal search form and search for your data.
You do not need the folders listed above for this simplified compose configuration.

The docker compose does depend on the two docker images `fitsstorageutils:latest` and `fitsimage:latest`.
You can build these two images by running `FitsStorage/docker/scripts/buildfitsstorageutils.sh` and
`FitsStorage/docker/scripts/buildarchive.sh` respectively.  The images are also available in the container
registry in gitlab at `https://gitlab.gemini.edu/DRSoftware/FitsStorage/container_registry`.
The scripts assume that `FitsStorageDB` and `GeminiCalMgr` are checked out alongside `FitsStorage`.

### Scripts

All of the Docker support is saved in the `docker/` folder in the project.  This consists
of a set of Dockerfiles to build various machine types and supporting scripts to
simplify spinning them up.

#### Postgres

We also have a container for running your own Postgres database.  This allows you to 
operate against your system without impacting any of the shared environments.  This is
even more useful if you are making any schema changes.  For postgres, we use the
official Postgres image and just supply appropriate arguments for the network, database
name and a login.

```
./docker/scripts/postgres.sh
```

#### Build Images

The docker support for running a cluster depends on some custom images.  There
are helper build scripts to easily create these with the proper parameters and
names.  These scripts assume `FitsStorageDB` and `GeminiCalMgr` are checked
out alongside `FitsStorage`.

If you are on the master branch and want to update the `:latest` images in the
gitlab repo, add a `-u` to these scripts when you run them.

```
bash ./FitsStorage/docker/scripts/buildfitsstorageutils.sh
bash ./FitsStorage/docker/scripts/buildarchive.sh
```

#### PostgreSQL Databases

For the cluster, we setup two postgres databases.  One of these is used by our
'onsite' website and the other is used by our 'public' archive website.  We use
the off-the-shelf PostgreSQL docker image from dockerhub and we run our 
`create_tables.py` on each to initialized them.  Running `create_tables.py` is
done by using a one shot container based on the `fitsstorageutils` image
created above.

All of this is wrapped up in a helper script, so you can simply do:

```
bash ./docker/scripts/postgres.sh
```

#### Onsite Website

We can setup a container to run as if it is the 'onsite' website.  This is the
one hosted internally at Gemini North or South and is distinct from the public
facing 'Archive Website'.  To run this, you can use the helper script:

```
bash ./FitsStorage/docker/scripts/onsite.sh
```

#### Archive Website

Now that you have initialized the database, you can run the website in a
container as well.  To do this, run:

```
bash ./FitsStorage/docker/scripts/api.sh
bash ./FitsStorage/docker/scripts/archive.sh
```

This container will expose the website on port 8080 on your host machine.
So, you can now browse to `http://localhost:8080/searchform` and see
the website.  You won't have any data since you haven't ingested any yet.

### Cleanup

After you are up and running, you can clean everything up at any time
by running the cleanup script.  This stops and removes all of the above
containers and it clears out your data folders, save for the one you put 
your test data in at the beginning.

```
bash ./FitsStorage/docker/scripts/cleanup.sh
```

### EPEL Notes

Here are the commands in `fitsstorage-jenkins/Dockerfile` 
we needed for the workaround EPEL repo:

```
# ENABLE EPEL
RUN rm -r /var/cache/dnf
RUN dnf -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
```
