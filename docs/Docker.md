# Docker Dev Setup

FitsStorage can be run via Docker containers.  This has the advantage of being closer to
what the system will be like in the deployed servers, with matching OS and layouts.  It
also makes it very easy to clean up and start from a known state.  Finally, this leverages
the same docker support we are using for doing CI/CD testing.

## Getting Started

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

## Folders

The containers built by our helper scripts depend on a few folders.  It is
best if you create these folders now and toss some sample data into one of 
them for ingest later.  These folders are:

```
mkdir -p ~/testdata/archive-data-upload_staging
mkdir -p ~/testdata/archive-sci-dataflow
mkdir -p ~/testdata/onsite-data-upload_staging
mkdir -p ~/testdata/onsite-sci-dataflow
```

Copy your datafiles into `~/testdata/onsite-sci-dataflow`

## Compose

There is now a very fast way to get a test cluster up complete with two webservers for
'onsite' and 'archive' (public).  You can just use docker-compose.  There is a `docker-compose.yml`
file in the top directory of the tree.  Just change to the top folder of your checkout and
run `docker-compose up`.  Instructions for running containers manually are listed below in case
you don't want to use docker-compose.

## Scripts

All of the Docker support is saved in the `docker/` folder in the project.  This consists
of a set of Dockerfiles to build various machine types and supporting scripts to
simplify spinning them up.

### Postgres

We also have a container for running your own Postgres database.  This allows you to 
operate against your system without impacting any of the shared environments.  This is
even more useful if you are making any schema changes.  For postgres, we use the
official Postgres image and just supply appropriate arguments for the network, database
name and a login.

```
./docker/scripts/postgres.sh
```

### Build Images

The docker support for running a cluster depends on some custom images.  There
are helper build scripts to easily create these with the proper parameters and
names.

```
bash ./docker/scripts/buildfitsstorageutils.sh
bash ./docker/scripts/buildarchive.sh
```

### PostgreSQL Databases

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

### Onsite Website

We can setup a container to run as if it is the 'onsite' website.  This is the
one hosted internally at Gemini North or South and is distinct from the public
facing 'Archive Website'.  To run this, you can use the helper script:

```
bash ./docker/scripts/onsite.sh
```

### Archive Website

Now that you have initialized the database, you can run the website in a
container as well.  To do this, run:

```
bash ./docker/scripts/api.sh
bash ./docker/scripts/archive.sh
```

This container will expose the website on port 8080 on your host machine.
So, you can now browse to `http://localhost:8080/searchform` and see
the website.  You won't have any data since you haven't ingested any yet.

## Cleanup

After you are up and running, you can clean everything up at any time
by running the cleanup script.  This stops and removes all of the above
containers and it clears out your data folders, save for the one you put 
your test data in at the beginning.

```
bash ./docker/scripts/cleanup.sh
```