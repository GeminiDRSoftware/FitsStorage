#!/usr/bin/env python

from optparse import OptionParser
from fits_storage.orm import sessionfactory
from fits_storage.orm.createtables import create_tables, drop_tables

# Option Parsing
parser = OptionParser()
parser.add_option("--drop", action="store_true", dest="drop", help="Drop the tables first")
parser.add_option("--nocreate", action="store_true", dest="nocreate", help="Do not actually create the tables")

(options, args) = parser.parse_args()


session = sessionfactory()

if options.drop:
    print "Dropping database tables"
    drop_tables(session)

if not options.nocreate:
    print "Creating database tables"
    create_tables(session)

session.close()

print "You may now want to ingest the standard star list"
