import smtplib
from io import BufferedReader, BytesIO

import pytest
from sqlalchemy.orm.exc import NoResultFound

from fits_storage.orm.ingestqueue import IngestQueue
from fits_storage.orm.user import User
from fits_storage.utils.null_logger import EmptyLogger
from fits_storage.utils.web import get_context
from fits_storage.utils.web.wsgi_adapter import Request, Response
from fits_storage.web.user import request_account, password_reset, request_password_reset, change_email, \
    change_password, staff_access, admin_change_email, login

from smtplib import SMTP

from fits_storage import fits_storage_config


def _mock_sendmail(fromaddr, toaddr, message):
    pass


def _init_gmos(session):
    session.rollback()
    # try:
    #     user = session.query(User).filter(User.username == 'ooberdorf').one()
    #     user.change_password('p4$$Word4pytest')
    #     user.email = 'ooberdorf@gemini.edu'
    # except NoResultFound as nrf:
    #     user = User('ooberdorf')
    #     user.email = 'ooberdorf@gemini.edu'
    #     user.change_password('p4$$Word4pytest')
    #     session.add(user)


@pytest.mark.usefixtures("rollback")
def test_standard(session):
    _init_gmos(session)
    save_storage_root = fits_storage_config.storage_root
    try:
        # TODO work in progress
        # TODO migrate to some sort of config singleton that we can easily customize for pytests
        fits_storage_config.storage_root='.'
        # print("storage root: %s" % fits_storage_config.storage_root)
        # iq = IngestQueue(session, EmptyLogger())
        # iq.ingest_file("somegmosdata.fits", "", False, False)
    finally:
        fits_storage_config.storage_root = save_storage_root
