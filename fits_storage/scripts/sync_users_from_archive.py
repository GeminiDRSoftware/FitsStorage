#! /usr/bin/env python3

from argparse import ArgumentParser
import datetime
import requests

from fits_storage.logger import logger, setdebug, setdemon

from fits_storage.db import sessionfactory
from fits_storage.server.orm.user import User

from fits_storage.config import get_config
fsc = get_config()


parser = ArgumentParser(prog='sync_users_from_archive.py',
                        description='Fetch local user info from archive and '
                                    'update local user database')
parser.add_argument("--server", action="store", dest="server",
                    default="https://archive.gemini.edu",
                    help="Remote server to fetch user data from")
parser.add_argument("--dryrun", action="store_true", dest="dryrun",
                    default=False, help="Don't actually update local database")
parser.add_argument("--debug", action="store_true", dest="debug",
                    default=False, help="Increase log level to debug")
parser.add_argument("--demon", action="store_true", dest="demon", default=False,
                    help="Run as background demon, do not generate stdout")
args = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(args.debug)
setdemon(args.demon)

# Announce startup
logger.info("***   sync_users_from_archive.py - starting up at %s",
            datetime.datetime.now())
logger.debug("Config files used: %s", ', '.join(fsc.configfiles_used))

# Fetch data from archive
url = args.server + "/export_users"
logger.debug("Fetching user data from: %s", url)

cookie = fsc.get('gemini_user_transfer')
if cookie is None:
    logger.error("gemini_user_transfer not set in config. Aborting.")
    exit(1)
cookies = {'gemini_user_transfer': cookie}

r = requests.get(url, cookies=cookies)

if r.status_code != 200:
    logger.error("Got bad status code from server: %s. Aborting.",
                 r.status_code)
    exit(2)

user_list = r.json()

# Parse list
if len(user_list) == 0:
    logger.info("Got zero users from server. Aborting.")
    exit(3)
else:
    logger.info("Got %d users from remote server", len(user_list))

session = sessionfactory()
for user_dict in user_list:
    # For convenience
    id = user_dict['id']
    username = user_dict['username']
    # Convert to datetime instances
    if user_dict.get('account_created') is not None:
        user_dict['account_created'] = \
            datetime.datetime.fromisoformat(user_dict['account_created'])
    if user_dict.get('password_changed') is not None:
        user_dict['password_changed'] = \
            datetime.datetime.fromisoformat(user_dict['password_changed'])

    user = session.get(User, id)
    if user is None:
        logger.info("Creating local user id %d for %s", id, username)
        user = User(username)
        for key in user_dict.keys():
            setattr(user, key, user_dict[key])
        if args.dryrun:
            logger.info("Dry-run mode, not actually adding user")
        else:
            try:
                session.add(user)
                session.commit()
            except Exception:
                logger.error("Exception adding new user to local database",
                             exc_info=True)
    else:
        logger.debug("Checking existing user id %d for updates", id)
        if user.username != username:
            logger.warning("Username for user id %d changed from %s to %s! "
                           "This is unexpected, but continuing regardless",
                           id, user.username, username)
        update = False
        for key in user_dict.keys():
            if getattr(user, key) != user_dict[key]:
                logger.info("Updating %s for user id %d - %s",
                            key, id, username)
                setattr(user, key, user_dict[key])
                update = True
        if update:
            if args.dryrun:
                logger.info("Dry-run mode, not actually updating database")
                session.rollback()
            else:
                try:
                    session.commit()
                except Exception:
                    logger.error("Error updating user id %d - %s", id, username)
                    session.rollback()
        else:
            logger.info("No updates for user id %d - %s", id, username)

logger.info("***   sync_users_from_archive.py - exiting normally up at %s",
            datetime.datetime.now())
