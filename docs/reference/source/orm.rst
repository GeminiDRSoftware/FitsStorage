Object-Relational Mapper
========================

The archive uses `SQLAlchemy <http://sqlalchemy.org>`_ (v1.0+) as its ORM engine. The
chosen driver is `psycopg <http://initd.org>`_, for
`PostgreSQL <http://postgresql.org>`_. The minimum supported Postgres version
is 8.4 which should be good for any reasonably recent GNU/Linux distribution
-RHEL/Centos 6 and Debian Squeeze package it, meaning that anything post-2010
should do.

User Object
-----------

Instances for :py:class:`User` give information about logged in users, including
details like their email, authorization level, etc. The current logged user can be
retrieved from at any time from the environment by using the
:any:`userfromcookie` function.

.. autoclass:: fits_storage.orm.user.User
   :members:
