#!/usr/bin/env python

from argparse import ArgumentParser

from gemini_obs_db import session_scope
from fits_storage.orm.user import User
from fits_storage.orm.createtables import create_tables, drop_tables

from datetime import datetime

"""
Helper script for creating a user for the FITS Server or Archive
"""

if __name__ == "__main__":

    # ------------------------------------------------------------------------------
    # Option Parsing
    parser = ArgumentParser()
    parser.add_argument("--username", action="store", dest="username", type=str,
                        help="Drop the tables first")
    parser.add_argument("--password", action="store", dest="password", type=str,
                        help="Drop the tables first")
    parser.add_argument("--email", action="store", dest="email", type=str,
                        help="Specify the email (should be unique)")
    parser.add_argument("--fullname", action="store", dest="fullname", type=str,
                        help="Specify the full name of the user (defaults to username)")
    parser.add_argument("--superuser", action="store_true", dest="superuser",
                        help="Mark user as Gemini staff (superuser)")

    args = parser.parse_args()

    if not args.username:
        print("Need to pass a --username")
        exit(1)

    if not args.password:
        print("Need to pass a --password")
        exit(2)


    # ------------------------------------------------------------------------------
    with session_scope() as session:
        user = session.query(User).filter_by(username=args.username).first()
        if user is None:
            user = User(args.username)
            session.add(user)

        if args.superuser:
            user.gemini_staff = True
            user.superuser = True
        if args.email:
            user.email = args.email
        else:
            user.email = "%s@gemini.edu" % args.username
        if args.fullname:
            user.fullname = args.fullname
        else:
            user.fullname = args.username
        user.account_created = datetime.now()

        # now set the password
        if args.password:
            password = args.password
        else:
            password = "changeme"
        user.reset_password(password)
        user.password_changed = datetime.now()

        session.commit()

    print("Created user: %s" % args.username)
