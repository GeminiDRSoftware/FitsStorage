import smtplib
from io import BufferedReader, BytesIO

import pytest
from sqlalchemy.orm.exc import NoResultFound

from fits_storage.orm.user import User
from fits_storage.utils.web import get_context
from fits_storage.utils.web.wsgi_adapter import Request, Response
from fits_storage.web.user import request_account, password_reset, request_password_reset, change_email, \
    change_password, staff_access, admin_change_email, login

from smtplib import SMTP


def _mock_sendmail(fromaddr, toaddr, message):
    pass


def _init_test_user(session):
    session.rollback()
    try:
        user = session.query(User).filter(User.username == 'ooberdorf').one()
        user.change_password('p4$$Word4pytest')
        user.email = 'ooberdorf@gemini.edu'
    except NoResultFound as nrf:
        user = User('ooberdorf')
        user.email = 'ooberdorf@gemini.edu'
        user.change_password('p4$$Word4pytest')
        session.add(user)
        session.flush()


def _init_admin_user(session):
    session.rollback()
    try:
        user = session.query(User).filter(User.username == 'phirst').one()
        user.email = 'noreply@gemini.edu'
        user.superuser = True
    except NoResultFound as nrf:
        user = User('phirst')
        user.email = 'noreply@gemini.edu'
        user.superuser = True
        session.add(user)
        session.flush()


@pytest.mark.usefixtures("rollback")
def test_request_account(session):
    _init_test_user(session)
    environ = {"wsgi.input": "{}"}
    start_response = {}
    ctx = get_context(initialize=True)
    request = Request(session, environ)
    response = Response(session, environ, start_response)
    ctx.setContent(request, response)
    things = {}
    request_account(things)
    assert(ctx.resp.status == 200)


@pytest.mark.usefixtures("rollback")
def test_request_account_submit(session, monkeypatch):
    user = session.query(User).filter(User.username == 'ooberdorf').one()
    session.delete(user)
    # TODO figure out why the monkeypatch doesn't take
    monkeypatch.setattr(SMTP, 'sendmail', _mock_sendmail)
    environ = {"REQUEST_METHOD": "POST",
               "wsgi.input": BufferedReader(BytesIO(b"username=ooberdorf&fullname=Oliver Oberdorf&email=ooberdorf@gemini.edu"))}
    start_response = {}
    ctx = get_context(initialize=True)
    request = Request(session, environ)
    response = Response(session, environ, start_response)
    ctx.setContent(request, response)
    things = {}
    request_account(things)
    assert(ctx.resp.status == 200)
    user = session.query(User).filter(User.username == 'ooberdorf').one()
    assert(user.reset_active)
    assert(user.reset_requested)


@pytest.mark.usefixtures("rollback")
def test_password_reset(session, monkeypatch):
    _init_test_user(session)
    # TODO figure out why the monkeypatch doesn't take
    monkeypatch.setattr(SMTP, 'sendmail', _mock_sendmail)
    environ = {"REQUEST_METHOD": "POST",
               "wsgi.input": BufferedReader(BytesIO(b"username=ooberdorf&email=ooberdorf@gemini.edu&thing=ooberdorf"))}
    start_response = {}
    ctx = get_context(initialize=True)
    request = Request(session, environ)
    response = Response(session, environ, start_response)
    ctx.setContent(request, response)
    things = {}
    request_password_reset()
    assert(ctx.resp.status == 200)
    user = session.query(User).filter(User.username == 'ooberdorf').one()
    assert(user.reset_active)
    assert(user.reset_requested)

    # environ = {"REQUEST_METHOD": "POST",
    #            "wsgi.input": BufferedReader(BytesIO(b"username=ooberdorf&email=ooberdorf@gemini.edu&thing=ooberdorf"))}
    # start_response = {}
    # ctx = get_context(initialize=True)
    # request = Request(session, environ)
    # response = Response(session, environ, start_response)
    environ["wsgi.input"] = BufferedReader(BytesIO(b"password=p4$$Word4pytest&again=p4$$Word4pytest"))
    password_reset(user.id, user.reset_token)
    assert(ctx.resp.status == 200)


@pytest.mark.usefixtures("rollback")
def test_change_email(session, monkeypatch):
    _init_test_user(session)
    # TODO figure out why the monkeypatch doesn't take
    monkeypatch.setattr(SMTP, 'sendmail', _mock_sendmail)
    environ = {"REQUEST_METHOD": "POST",
               "wsgi.input": BufferedReader(BytesIO(b"thing=ooberdorf&newemail=oberdorf.oliver@gmail.com&newagain=oberdorf.oliver@gmail.com"))}
    start_response = {}
    ctx = get_context(initialize=True)
    ctx.user = session.query(User).filter(User.username == 'ooberdorf').one()
    request = Request(session, environ)
    response = Response(session, environ, start_response)
    ctx.setContent(request, response)
    things = {}
    change_email(things)
    assert(ctx.resp.status == 200)
    user = session.query(User).filter(User.username == 'ooberdorf').one()
    assert(user.email == 'oberdorf.oliver@gmail.com')


@pytest.mark.usefixtures("rollback")
def test_change_password(session, monkeypatch):
    _init_test_user(session)

    # ensure we know the "old password"
    user = session.query(User).filter(User.username == 'ooberdorf').one()
    user.change_password("p4$$Word4pytest")
    # TODO figure out why the monkeypatch doesn't take
    monkeypatch.setattr(SMTP, 'sendmail', _mock_sendmail)
    environ = {"REQUEST_METHOD": "POST",
               "wsgi.input": BufferedReader(BytesIO(b"oldpassword=p4$$Word4pytest&newpassword=p4$$Word4pytest2&newagain=p4$$Word4pytest2"))}
    start_response = {}
    ctx = get_context(initialize=True)
    ctx.user = session.query(User).filter(User.username == 'ooberdorf').one()
    request = Request(session, environ)
    response = Response(session, environ, start_response)
    ctx.setContent(request, response)
    things = {}
    change_password(things)
    assert(ctx.resp.status == 200)
    # user = session.query(User).filter(User.username == 'ooberdorf').one()
    # assert(user.email == 'oberdorf.oliver@gmail.com')


@pytest.mark.usefixtures("rollback")
def test_staff_access(session, monkeypatch):
    _init_test_user(session)
    _init_admin_user(session)

    # ensure we know the "old password"
    user = session.query(User).filter(User.username == 'ooberdorf').one()
    user.superuser = False

    # TODO figure out why the monkeypatch doesn't take
    environ = {"REQUEST_METHOD": "POST",
               "wsgi.input": BufferedReader(BytesIO(b"username=ooberdorf&action=Grant"))}
    start_response = {}
    ctx = get_context(initialize=True)
    ctx.user = session.query(User).filter(User.username == 'phirst').one()
    request = Request(session, environ)
    response = Response(session, environ, start_response)
    ctx.setContent(request, response)
    things = {}
    staff_access()
    assert(ctx.resp.status == 200)
    # user = session.query(User).filter(User.username == 'ooberdorf').one()
    # assert(user.email == 'oberdorf.oliver@gmail.com')


@pytest.mark.usefixtures("rollback")
def test_admin_change_email(session, monkeypatch):
    _init_test_user(session)
    _init_admin_user(session)

    # ensure we know the "old password"
    user = session.query(User).filter(User.username == 'ooberdorf').one()
    user.superuser = False

    # TODO figure out why the monkeypatch doesn't take
    environ = {"REQUEST_METHOD": "POST",
               "wsgi.input": BufferedReader(BytesIO(b"username=ooberdorf&email=oberdorf.oliver@gmail.com"))}
    start_response = {}
    ctx = get_context(initialize=True)
    ctx.user = session.query(User).filter(User.username == 'phirst').one()
    request = Request(session, environ)
    response = Response(session, environ, start_response)
    ctx.setContent(request, response)
    things = {}
    admin_change_email()
    assert(ctx.resp.status == 200)
    user = session.query(User).filter(User.username == 'ooberdorf').one()
    assert(user.email == 'oberdorf.oliver@gmail.com')


@pytest.mark.usefixtures("rollback")
def test_login(session, monkeypatch):
    _init_test_user(session)

    # TODO figure out why the monkeypatch doesn't take
    environ = {"REQUEST_METHOD": "POST",
               "wsgi.input": BufferedReader(BytesIO(b"username=ooberdorf&password=p4$$Word4pytest"))}
    start_response = {}
    ctx = get_context(initialize=True)
    request = Request(session, environ)
    response = Response(session, environ, start_response)
    ctx.setContent(request, response)
    things = {}
    login(things)
    assert(ctx.resp.status == 200)
    assert(response._cookies_to_send.get('gemini_archive_session') is not None)

    # TODO figure out why the monkeypatch doesn't take
    environ = {"REQUEST_METHOD": "POST",
               "wsgi.input": BufferedReader(BytesIO(b"username=ooberdorf&password=badpassword"))}
    start_response = {}
    ctx = get_context(initialize=True)
    request = Request(session, environ)
    response = Response(session, environ, start_response)
    ctx.setContent(request, response)
    things = {}
    login(things)
    assert(ctx.resp.status == 200)
    assert(response._cookies_to_send.get('gemini_archive_session') is None)
