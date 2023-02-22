#!/usr/bin/env python

from argparse import ArgumentParser

from fits_storage.db.createtables import create_tables, drop_tables

from fits_storage.db import session_scope

from fits_storage.config import get_config
fsc = get_config()

"""
Helper script for generating the initial database.
"""

if __name__ == "__main__":

    # Option Parsing
    parser = ArgumentParser()
    parser.add_argument("--drop", action="store_true", dest="drop",
                        help="Drop the tables first")
    parser.add_argument("--nocreate", action="store_true", dest="nocreate",
                        help="Do not actually create the tables")

    args = parser.parse_args()

    print("Loaded configuration from: ")
    for f in fsc.configfiles_used:
        print(f"  - {f}")
    print()
    print(f"Database URL is: {fsc.database_url}")
    print()

    with session_scope() as session:
        if args.drop:
            print("Dropping database tables")
            drop_tables(session)

        if not args.nocreate:
            print("Creating database tables")
            create_tables(session)

    print("You may now want to ingest the standard star list")
