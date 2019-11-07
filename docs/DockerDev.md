# Docker Dev Setup

FitsStorage can be run via Docker containers.  This has the advantage of being closer to
what the system will be like in the deployed servers, with matching OS and layouts.  It
also makes it very easy to clean up and start from a known state.  Finally, this leverages
the same docker support we are using for doing CI/CD testing.

## Getting Started

To begin, you will want to install the latest Docker on your machine.  I am using a
MacBook, so my instructions will be geared to OSX but Linux and Windows should also 
work.

In addition to docker, you /may/ want to install `sshfs`.  `sshfs` will allow you to
mount remote data folders onto your machine.  From there, you can map these folders
into the archive container where appropriate.  In other words, you could do this to
make it easier to restore a DB backup onto a fresh Postgres, or to be able to serve
file downloads from your dev archive without pulling down all the datafiles.
However, I would suggest getting your docker setup without it first and then come
back to it when you are ready.

## Scripts

All of the Docker support is saved in the `docker/` folder in the project.  This consists
of a set of Dockerfiles to build various machine types and supporting scripts to
simplify spinning them up.

### Network

The images all expect to run on a Docker network called `fitsstorage`.  This allows us to 
run the database, tools, and archive in separate containers yet still allow them to find
eachother.  To set this up, you need to do a one time creation of the network

```
docker network create fitsstorage
```

### Postgres

We also have a container for running your own Postgres database.  This allows you to 
operate against your system without impacting any of the shared environments.  This is
even more useful if you are making any schema changes.  For postgres, we use the
official Postgres image and just supply appropriate arguments for the network, database
name and a login.

```shell script
./docker/scripts/postgres.sh
```

After you have run the container like this, in future you can just use

```shell script
docker container stop postgres
docker container start postgres
```

to manage the instance.

### Utilities

Once you have the database up and running, you will want to run the FitsStorage 
utility to initialize it.  Per Docker best practices, we have separated this task
out into a dedicated container and not on the website.  As this is a custom
container, you have to build the image and then create the container.

```shell script
./docker/scripts/buildfitsstorageutils-centos8.sh
./docker/scripts/fitsstorageutils-centos8.sh
```

This container runs in the foreground and gives you a `bash` prompt.  It will
already be configured with an environment to point it to your new `postgres`
database.  You will be sitting in a folder with the FitsStorage codebase.
To initialize the database, simply run:

```shell script
python ./fits_storage/scripts/create_tables.py
```

When you exit the bash shell, the container will be cleaned up.  So, for the
utilities you will wan to run `fitsstorageutils-centos8.sh` any time you want
to use them in future.

### Archive Website

Now that you have initialized the database, you can run the website in a
container as well.  Again, this is custom so we build the image and then
create a container based on that.  To do this, run:

```shell script
./docker/scropts/buildarchive-centos8.sh
./docker/scripts/archive-centos8.sh
```

This container will expose the website on port 8080 on your host machine.
So, you can now browse to `http://localhost:8080/searchform` and see
the website.  You won't have any data since you haven't ingested any yet.

## Ingesting Data

To be useful, you'll likely want to ingest some data into the website.  We
can do this manually from the utilities container, but we'll need to 
map in a folder with data to load.

The utilities container maps `~/dataflow` to `/sci/dataflow`.  So, if
you place files in `~/dataflow` you can ingest them.  To do this, start
the utilities container again with `fitsstorageutils-centos8.sh` and
run this at the prompt:

```shell script
python fits_storage/scripts/add_to_ingest_queue.py
python fits_storage/scripts/service_ingest_queue.py --empty
```

The first command should queue up all of your data files for ingest.  The
second command runs the ingest as a one-shot.  Once it finishes, you should
see the data in your website.
