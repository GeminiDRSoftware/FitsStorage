This is a database migration repository.

More information at
http://code.google.com/p/sqlalchemy-migrate/

## Setting Up Migration

sudo -u postgres  env PYTHONPATH=/opt/DRAGONS:/opt/FitsStorage python3 dbmigration/manage.py version_control postgresql:///fitsdata dbmigration

## Performing Upgrades

sudo -u postgres env PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS python3 dbmigration/manage.py upgrade postgresql:///fitsdata dbmigration

Note that you also want to ensure apache and fitsdata users have permission to access the tables and sequences:

```
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fitsdata;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO fitsdata;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO apache;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO apache;
```
