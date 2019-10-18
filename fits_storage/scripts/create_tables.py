#!/usr/bin/env python

from argparse import ArgumentParser

from fits_storage.orm import session_scope
from fits_storage.orm.createtables import create_tables, drop_tables

# ------------------------------------------------------------------------------
# Option Parsing
parser = ArgumentParser()
parser.add_argument("--drop", action="store_true", dest="drop",
                    help="Drop the tables first")
parser.add_argument("--nocreate", action="store_true", dest="nocreate",
                    help="Do not actually create the tables")

args = parser.parse_args()

# ------------------------------------------------------------------------------
with session_scope() as session:
    if args.drop:
        print("Dropping database tables")
        drop_tables(session)

    if not args.nocreate:
        print("Creating database tables")
        create_tables(session)

print("You may now want to ingest the standard star list")
