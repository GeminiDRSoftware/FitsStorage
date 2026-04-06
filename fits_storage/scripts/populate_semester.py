#! /usr/bin/env python3

import datetime

from fits_storage.db import sessionfactory

from fits_storage.server.orm.semester import Semester

from fits_storage.config import get_config
fsc = get_config()

"""
Populate the list of semesters. We assume standard dates rather than accounting
for actual switchover dates each year.
20xxA = 20xx-02-01 - 20xx-07-31
20xxB = 20xx-08-01 - 20(xx+1)-01-31

These are given as Dates rather than DateTimes, with the start of a 
semester being the same as the end of the previous one. This should
facilitate correct handling of Chile nights at the boundary.
"""

if __name__ == "__main__":
    session = sessionfactory()

    year = 2000

    while year < 2030:

        astart = datetime.date(year, 2, 1)
        aend = datetime.date(year, 8, 1)
        bstart = aend
        bend = datetime.date(year+1, 2, 1)

        a = Semester(f'{year}A', astart, aend)
        b = Semester(f'{year}B', bstart, bend)
        session.add(a)
        session.add(b)
        session.commit()
        year += 1