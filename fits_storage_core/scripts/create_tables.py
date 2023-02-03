#!/usr/bin/env python

from argparse import ArgumentParser

from gemini_obs_db.utils.createtables import create_tables, drop_tables

from gemini_obs_db.db import session_scope
from gemini_obs_db import db_config as dbc

"""
Helper script for generating the initial database.
"""

if __name__ == "__main__":

    # ------------------------------------------------------------------------------
    # Option Parsing
    parser = ArgumentParser()
    parser.add_argument("--drop", action="store_true", dest="drop",
                        help="Drop the tables first")
    parser.add_argument("--nocreate", action="store_true", dest="nocreate",
                        help="Do not actually create the tables")
    parser.add_argument("--url", action="store", dest="url",
                        help="Database URL for SqlAlchemy", default=dbc.database_url)

    args = parser.parse_args()
    dbc.database_url = args.url  # set this before we get the session

    # ------------------------------------------------------------------------------
    with session_scope() as session:
        if args.drop:
            print("Dropping database tables")
            drop_tables(session)

        if not args.nocreate:
            print("Creating database tables")
            create_tables(session)

    print("You may now want to ingest the standard star list")
