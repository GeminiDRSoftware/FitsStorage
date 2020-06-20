# Postgres Database Creation

Note this is done by ansible automatically on install.

## Create Users

To create the database users and databases:

```
sudo su - postgres
/usr/bin/createuser --no-superuser --no-createrole --createdb fitsdata
/usr/bin/createuser --no-superuser --no-createrole --no-createdb apache
exit
```

## Create Database

now as fitsuser again:

`/usr/bin/createdb fitsdata`

## create_tables.py

the rest is handled by the create_table.py script
