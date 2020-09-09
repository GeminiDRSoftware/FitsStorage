"""
Script to find potential issues in various systems.

This works through a variety of checks.  All checks should complete in reasonable time.
This is not the place to put exhaustive all-dataflow scans or decompress 100s of bz2
files.  We are just looking for high-level stuff here.
"""
import datetime
import os
import smtplib
from optparse import OptionParser

import re

from abc import ABC, abstractmethod

from fits_storage.fits_storage_config import smtp_server, dhs_perm
from fits_storage.orm import session_scope
from fits_storage.orm.diskfile import DiskFile
from fits_storage.orm.header import Header

_alopeke_staging = "/net/mkovisdata/home/alopeke"
_zorro_staging = "/net/cpostonfs-nv1/tier2/ins/sto/zorro"
_igrins_staging = "/net/cpostonfs-nv1/tier2/ins/sto/igrins/DATA"


class ProblemChecker(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def name(self):
        """
        Return a unique name for this problem checker
        """
        return None

    @abstractmethod
    def applicable(self):
        """
        Implement this to return if we are relevant on the system
        """
        return False


class AlopekeZorroProblemChecker(ProblemChecker, ABC):
    def __init__(self):
        super().__init__()
        # self._date_re = re.compile(r'[A-Z]{4}_(\d{8})_\d{4}.*\.fits')
        self._dir_re = re.compile(r'(\d{4})(\d{2})(\d{2})')

    def applicable(self):
        if os.path.exists(self._staging_dir):
            return True
        return False

    def check_problems(self, session):
        for f in os.listdir(self._staging_dir):
            m = self._dir_re.match(f)
            if m:
                dirname = m.group(0)
                year = int(m.group(1))
                month = int(m.group(2))
                day = int(m.group(3))
                if (datetime.datetime.now() - datetime.datetime(year, month, day)) < datetime.timedelta(days=4):
                    for f in os.listdir(os.path.join(self._staging_dir, dirname)):
                        if self._filename_re.match(f):
                            query = session.query(DiskFile) \
                                .filter(DiskFile.canonical == True). \
                                filter(DiskFile.filename == f)

                            record = query.first()

                            if record is None:
                                yield "checker: %s, filename: %s, problem: No canonical DiskFile found" % (
                                self.name(), f)
                            else:
                                query = session.query(Header) \
                                    .filter(Header.diskfile_id == record.id)
                                record = query.first()
                                if record is None:
                                    yield "checker: %s, filename': %s, problem: Missing Header (but Diskfile exists)" % (
                                    self.name(), f)


class AlopekeProblemChecker(AlopekeZorroProblemChecker):
    def __init__(self):
        super().__init__()
        self._filename_re = re.compile(r'N\d{8}A\d{4}[br].fits.bz2')
        self._staging_dir = _alopeke_staging

    def name(self):
        return "alopeke"


class ZorroProblemChecker(AlopekeZorroProblemChecker):
    def __init__(self):
        super().__init__()
        self._filename_re = re.compile(r'S\d{8}Z\d{4}[br].fits.bz2')
        self._staging_dir = _zorro_staging

    def name(self):
        return "zorro"


class IGRINSProblemChecker(ProblemChecker, ABC):
    def __init__(self):
        super().__init__()
        # self._date_re = re.compile(r'[A-Z]{4}_(\d{8})_\d{4}.*\.fits')
        self._filename_re = re.compile(r'S[A-Z]{3}_\d{8}_\d{4}.fits')
        self._semester_re = re.compile(r'(\d{4})[AB]')
        self._dir_re = re.compile(r'(\d{4})(\d{2})(\d{2})')
        self._staging_dir = _igrins_staging

    def applicable(self):
        if os.path.exists(self._staging_dir):
            return True
        return False

    def check_problems(self, session):
        for f in os.listdir(self._staging_dir):
            m = self._semester_re.match(f)
            if m:
                for datefolder in os.listdir(os.path.join(self._staging_dir, f)):
                    m = self._dir_re.match(datefolder)
                    if m:
                        dirname = m.group(0)
                        year = int(m.group(1))
                        month = int(m.group(2))
                        day = int(m.group(3))
                        if (datetime.datetime.now() - datetime.datetime(year, month, day)) < datetime.timedelta(days=4):
                            for f in os.listdir(os.path.join(self._staging_dir, f, dirname)):
                                if self._filename_re.match(f):
                                    query = session.query(DiskFile) \
                                        .filter(DiskFile.canonical == True). \
                                        filter(DiskFile.filename == f)

                                    record = query.first()

                                    if record is None:
                                        yield "checker: %s, filename: %s, problem: No canonical DiskFile found" % (
                                        self.name(), f)
                                    else:
                                        query = session.query(Header) \
                                            .filter(Header.diskfile_id == record.id)
                                        record = query.first()
                                        if record is None:
                                            yield "checker: %s, filename': %s, problem: Missing Header (but Diskfile exists)" % (
                                            self.name(), f)


class DHSProblemChecker(ProblemChecker):
    def __init__(self):
        super().__init__()
        # self._date_re = re.compile(r'[A-Z]{4}_(\d{8})_\d{4}.*\.fits')
        self._file_re = re.compile(r'[NS](\d{4})(\d{2})(\d{2}).*\.fits')
        self._staging_dir = dhs_perm

    def name(self):
        return "dhs"

    def applicable(self):
        if os.path.exists(self._staging_dir):
            return True
        return False

    def check_problems(self, session):
        for f in os.listdir(self._staging_dir):
            m = self._file_re.match(f)
            if m:
                year = int(m.group(1))
                month = int(m.group(2))
                day = int(m.group(3))
                if (datetime.datetime.now() - datetime.datetime(year, month, day)) < datetime.timedelta(days=4):
                    query = session.query(DiskFile) \
                        .filter(DiskFile.canonical == True). \
                        filter(DiskFile.filename == f)

                    record = query.first()

                    if record is None:
                        yield "checker: %s, filename: %s, problem: No canonical DiskFile found" % (self.name(), f)
                    else:
                        query = session.query(Header) \
                            .filter(Header.diskfile_id == record.id)
                        record = query.first()
                        if record is None:
                            yield "checker: %s, filename': %s, problem: Missing Header (but Diskfile exists)" % (self.name(), f)


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--emailto", action="store", type="string", dest="emailto",
                      help="Email address to send message to")
    (options, args) = parser.parse_args()

    problem_checkers = [
        AlopekeProblemChecker(),
        ZorroProblemChecker(),
        DHSProblemChecker(),
    ]

    print("Starting problem_checker.py")

    problems = list()

    with session_scope() as session:
        for problem_checker in problem_checkers:
            if problem_checker.applicable():
                problems.extend(problem_checker.check_problems(session))

    if options.emailto:
        subject = "Problem Checker Report"
        mailfrom = 'fitsdata@gemini.edu'
        mailto = [options.emailto]

        errmessage = None
        if problems:
            message = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % (
                mailfrom, ", ".join(mailto), "ERRORS - %s" % subject, '\n'.join(problems))

            server = smtplib.SMTP(smtp_server)
            server.sendmail(mailfrom, mailto, message)
            server.quit()
