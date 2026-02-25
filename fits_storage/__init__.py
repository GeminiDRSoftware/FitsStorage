# Don't forget to also update pyproject.toml when you update this
__version__ = '3.6.2-dev'

# Convenience utcnow() function since this was deprecated in python 3.12, and
# we use this a lot with timezone-naieve database columns that are defined to
# be in UTC
import datetime
def utcnow():
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
