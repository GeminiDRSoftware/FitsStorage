Database Notes
=============

The database for the FITS Servers run on the same host.  These are Postgres databases owned by the `fitsdata` user and
the name of the database is `fitsdata`.

Migration Scripts
-----------------

The database schema is managed with migration scripts.  These are numbered scripts that run in order to upgrade or
downgrade a database to a specific version.  This uses the `sqlalchemy-migrate` package and you can read more about
that here: http://code.google.com/p/sqlalchemy-migrate/


Migration Initialization
^^^^^^^^^^^^^^^^^^^^^^^^

Normally, this is done by the ansible deploy scripts.  The deploy will create a schema that matches the current
data models so we also create the `migrate_version` table described below and initialize it with the latest version
number.  Going forward, you can then do updates against that database as new schema deltas are added.


Check Current Version
^^^^^^^^^^^^^^^^^^^^^

You can see the current version of a database schema by looking at the
`migrate_version` table.

.. code:: sql

   select * from migrate_version;


Update Schema
^^^^^^^^^^^^^

You can update a schema with the `dbmigration/manage.py` script.  This will check the current version and run each
sequential migrate script necessary through the latest version.

.. code:: bash

   sudo -u postgres env PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS python3 dbmigration/manage.py upgrade postgresql:///fitsdata dbmigration

Currently, I am having issues with enum updates.  If there is an enum update script and you can't get it to work,
you may need to manually make that change and then update the database version in the `migrate_version` table to match.
In this case, you can update to the version just prior to the enum by specifying that specific version.  For example:

.. code:: bash

   sudo -u postgres env PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS python3 dbmigration/manage.py upgrade 18 postgresql:///fitsdata dbmigration


Downgrade Schema
^^^^^^^^^^^^^^^^

You can downgrade a schema as well.  Note that enum values can't be removed, so those will be left in place.  You need
to specify a target when downgrading.  You also need all of the current version scripts available.  So, for example, if
you have deployed via ansible and you want to rollback to an earlier build, you want to check out the deployed branch
first to do this downgrade.  Then check out the code for the version you want (and that you rolled back to here).

.. code:: bash

   sudo -u postgres env PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS python3 dbmigration/manage.py downgrade 18 postgresql:///fitsdata dbmigration

