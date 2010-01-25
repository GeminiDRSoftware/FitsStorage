from FitsStorageConfig import *
import os
import datetime

datestring = datetime.datetime.now().isoformat()

# The backup filename
filename = "%s.%s.pg_dump_c" % (fits_dbname, datestring)

command = "/usr/bin/pg_dump --format=c --file=%s/%s %s" % (fits_db_backup_dir, filename, fits_dbname)

print "executing command: %s" % command

os.system(command)
