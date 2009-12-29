import sys
sys.path += ['/opt/sqlalchemy/lib/python2.5/site-packages']

from FitsStorageUtils import *

session = sessionfactory()

create_tables(session)

session.close()

