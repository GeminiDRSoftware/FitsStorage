import json

from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound

from fits_storage.core.orm.header import Header
from fits_storage.queues.queue.reducequeue import ReduceQueue, memory_estimate

from fits_storage.logger_dummy import DummyLogger
from fits_storage.config import get_config
fsc = get_config()


def _matches(header, rule, newfile):
    # 'newfile' comes from the ingester rather than the header orm, it indicates
    # if this is a new file
    # If onlynew=True in the rule, we only match if newfile is True
    header.newfile = newfile

    matches = True
    for key, value in rule.items():
        if key == 'onlynew':
            if value is True and header.newfile is False:
                matches = False
        elif getattr(header, key) != value:
            matches = False
    return matches


class ReduceOnIngest(object):
    def __init__(self, rules_file=None, session=None, logger=DummyLogger()):
        self.rules = None
        self.rules_file = rules_file if rules_file is not None \
            else fsc.reduce_on_ingest_rules_file
        self.session = session
        self.logger = logger

        if self.rules_file:
            self.readrules()

    def readrules(self):
        self.rules = []
        try:
            self.logger.debug(f"Reading reduce rules file {self.rules_file}")
            with open(self.rules_file, 'r') as rf:
                self.rules = json.load(rf)
                self.logger.debug(f"Read {len(self.rules)} reduce rules")
        except FileNotFoundError:
            self.logger.error(f"Reduce on Ingest rules file {self.rules_file} "
                              f"Not Found")
        except Exception:
            self.logger.error("Could not read reduce on ingest rules file",
                              exc_info=True)

    def _get_header(self, diskfile):
        stmt = select(Header).where(Header.diskfile_id == diskfile.id)
        try:
            header = self.session.execute(stmt).scalar_one_or_none()
            return header
        except MultipleResultsFound:
            self.logger.error("MultipleResultsFound finding header for diskfile_id %d",
                              diskfile.id)
            return None

    def __call__(self, diskfile, newfile=False, header=None, rq=None):
        # Can pass a dummy reducequeue as rq for testing
        self.logger.debug("Reduce on Ingest file {diskfile.filename} {newfile=}")

        # Bail out if no rules defined
        if not self.rules:
            self.logger.warning("No reduce on ingest rules defined")
            return

        # This is called by ingester on anything that it just ingested.
        # Bail out if there's no header (e.g. miscfile, obslog, error)
        # Header is supplied as an argument for testing
        if header is None:
            header = self._get_header(diskfile)
        if header is None:
            self.logger.debug("Reduce-on-ingest: No header entry found")
            return

        if rq is None:
            rq = ReduceQueue(self.session, logger=self.logger)
        # Loop through the rules, see if we match
        for rule, action in self.rules:
            if _matches(header, rule, newfile):
                self.logger.info(f"Queuing for reduction under rule: {rule}")
                action['mem_gb'] = memory_estimate([header.numpix])
                rq.add([diskfile.filename], **action)

