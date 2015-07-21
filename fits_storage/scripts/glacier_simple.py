import datetime
import os
import sys

from fits_storage_config import aws_access_key, aws_secret_key
import boto.glacier

from logger import logger, setdebug, setdemon
#from orm import sessionfactory

# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
parser.add_option("--list-vaults", action="store_true", dest="list_vaults", help="List available Vaults and exit")
parser.add_option("--vault", action="store", dest="vault", default='gemini-archive', help="Name of Glacier vault to use")
parser.add_option("--upload", action="store", dest="upload", default=None, help="Upload this local file to the vault")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

# Annouce startup
logger.info("*********    glacier_simple.py - starting up at %s", datetime.datetime.now())


# Try to connect to glacier
glacier_connection = boto.connect_glacier(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name='us-west-2')
if glacier_connection is None:
    logger.info("Glacier connection failed.")
else:
    logger.info("Connected to Glacier.")

if options.list_vaults:
    # list Vaults 
    vaults = glacier_connection.list_vaults()
    logger.info("Found %d vaults", len(vaults))
    for vault in vaults:
        logger.info("Vault %s: created %s, last inventory: %s, size: %d", vault.name, vault.creation_date, vault.last_inventory_date, vault.size)
    logger.info("*********    glacier_simple.py - exiting normally at %s", datetime.datetime.now())
    sys.exit(0)

if options.vault is None:
    logger.info("You must specify a vault name. Exiting.")
    sys.exit(1)
else:
    vault = glacier_connection.get_vault(options.vault)

if options.upload:
    if os.path.isfile(options.upload):
        filename = options.upload
    else:
        logger.info("%s does not appear to be a file. Exiting", options.upload)
        sys.exit(2)
    
    logger.info("Uploading %s to vault %s", filename, vault.name)

    # Kludge for bug in boto
    vault.name = str(vault.name)
    archive_id = vault.upload_archive(filename, description=os.path.basename(filename))

    logger.info("Archive id for %s is: %s", filename, archive_id)
    

logger.info("*********    glacier_simple.py - exiting normally at %s", datetime.datetime.now())
