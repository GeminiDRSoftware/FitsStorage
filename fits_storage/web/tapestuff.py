"""
This module contains the tape related html generator functions. 
"""
from ..orm import sessionfactory, NoResultFound, MultipleResultsFound
from ..orm.tapestuff import Tape, TapeWrite, TapeFile, TapeRead
from . import templating

from ..utils.web import get_context

from sqlalchemy import join, desc, func

import datetime

@templating.templated("tapestuff/fileontape.xml", content_type='text/xml', with_generator=True)
def fileontape(filename):
    """
    Outputs xml describing the tapes that the specified file is on
    """

#    filename = things[0]

    query = (
        get_context().session.query(TapeFile).select_from(join(TapeFile, join(TapeWrite, Tape)))
                .filter(Tape.active == True).filter(TapeWrite.suceeded == True)
                .filter(TapeFile.filename == filename)
        )

    return dict(
        filelist = query
        )

@templating.templated("tapestuff/tape.html", with_generator=True)
def tape(search = None):
    """
    This is the tape list function
    """

    ctx = get_context()
    session = ctx.session

    # Process form data first
    formdata = ctx.get_form_data()
    for key, value in list(formdata.items()):
        field = key.split('-')[0]
        tapeid = int(key.split('-')[1])
        value = value.value

        if tapeid:
            tape = session.query(Tape).get(tapeid)
            if field == 'moveto':
                tape.location = value
                tape.lastmoved = datetime.datetime.utcnow()
            elif field == 'active':
                if value == 'Yes':
                    tape.active = True
                if value == 'No':
                    tape.active = False
            elif field == 'full':
                if value == 'Yes':
                    tape.full = True
                if value == 'No':
                    tape.full = False
            elif field == 'set':
                tape.set = value
            elif field == 'fate':
                tape.fate = value
        if field == 'newlabel':
            # Add a new tape to the database
            newtape = Tape(value)
            session.add(newtape)

        session.commit()

    # datetimes used to colour last verified warnings.
    now = datetime.datetime.utcnow()
    year = datetime.timedelta(days=365)
    nine = datetime.timedelta(days=int(365*0.75))
    bad = now - year
    warning = now - nine

    def generator():
        query = session.query(Tape)
        # Get a list of the tapes that apply
        if search:
            searchstring = '%'+search+'%'
            query = query.filter(Tape.label.like(searchstring))
        tapequery = query.order_by(desc(Tape.id))


        for tape in tapequery:
            # Count Writes
            twqtotal = session.query(TapeWrite).filter(TapeWrite.tape_id == tape.id)
            twq = session.query(TapeWrite).filter(TapeWrite.tape_id == tape.id).filter(TapeWrite.suceeded == True)
            # Count Bytes
            bytes = 0
            if twq.count():
                bytesquery = session.query(func.sum(TapeWrite.size)).filter(TapeWrite.tape_id == tape.id).filter(TapeWrite.suceeded == True)
                bytes = bytesquery.one()[0] or 0

            yield (tape, twqtotal.count(), twq.count(), float(bytes)/1.0E9)

    return dict(
        bad = bad,
        warning = warning,
        generator = generator(),
        )

@templating.templated("tapestuff/tapewrite.html", with_generator=True)
def tapewrite(label = None):
    """
    This is the tapewrite list function. Label may be an integer ID or a string
    """

    session = get_context().session

    # Find the appropriate TapeWrite entries
    query = session.query(TapeWrite, Tape).join(Tape)

    # Can give a tape id (numeric) or label as an argument
    if label:
        try:
            query = query.filter(TapeWrite.tape_id == int(label))
        except ValueError:
            label = '%'+label+'%'
            tapequery = session.query(Tape).filter(Tape.label.like(label))
            try:
                query = query.filter(Tape.id == tapequery.one().id)
            except NoResultFound:
                return dict(message = "Could not find tape by label search")
            except MultipleResultsFound:
                return dict(message = "Found multiple tapes by label search. Please give the ID instead")

    query = query.order_by(desc(TapeWrite.startdate))

    return dict(tws = query)

@templating.templated("tapestuff/tapefile.html", with_generator=True)
def tapefile(tapewrite_id):
    """
    This is the tapefile list function
    """

#    if not things:
#        return dict(message="Must supply one argument - tapewrite_id")

#    tapewrite_id = things[0]

    query = get_context().session.query(TapeFile).filter(TapeFile.tapewrite_id == tapewrite_id).order_by(TapeFile.id)

    return dict(tapefiles = query)

@templating.templated("tapestuff/taperead.html", with_generator=True)
def taperead():
    """
    This is the taperead list function
    """
    query = get_context().session.query(TapeRead).order_by(TapeRead.id)

    return dict(tapereads = query)
