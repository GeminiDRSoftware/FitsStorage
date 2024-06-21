"""
These handler functions deal with the data that comes back from the ODB.
The code in odb_program_interface.py talks to the ODB and generates a list of
dictionaries (one dictionary per program) that these functions use. The list
of dictionaries can be used directly, or can be passed as a JSON document
over HTTP to a remote server, which then uses this code to ingest the data.
"""

from fits_storage.logger import DummyLogger

from fits_storage.server.orm.notification import Notification
from fits_storage.server.orm.program import Program
from fits_storage.server.orm.obslog_comment import ObslogComment

# Do not remove this import, it's needed by SQLalchemy to initialize the
# Program ORM class because of the relationship() to Publication in there.
from fits_storage.server.orm.publication import Publication, ProgramPublication

from fits_storage.gemini_metadata_utils.progid_obsid_dl import GeminiProgram


def update_notifications(session, progdicts, logger=DummyLogger()):
    """
    Update notifications in the local Fits Server to match progdicts, which
    is a list of dictionaries containing the program info

    Parameters
    ----------
    session : :class:`sqlalchemy.orm.session.Session`
        SQL Alchemy session to operate in
    progdicts : list of dictionaries
            program information from ODB
    logger: logger-like instance or None
            logger to write messages to

    Returns
    -------
    None

    """
    nprogs = 0
    for prog in progdicts:
        nprogs += 1

        # Search for this program ID in notification table
        label = "Auto - %s" % prog['id']
        query = session.query(Notification).filter(Notification.label == label)
        if query.count() == 0:
            # This notification doesn't exist in DB yet.
            # Only add it if notifyPi is Yes, and it's a valid program ID
            gp = GeminiProgram(prog['id'])
            if prog['notify'] and gp.valid:
                n = Notification(label)
                n.selection = "%s/science" % prog['id']
                n.piemail = prog['piEmail']
                n.ngoemail = prog['ngoEmail']
                n.csemail = prog['csEmail']
                logger.info("Adding notification %s" % label)
                session.add(n)
                session.commit()
            else:
                if not gp.valid:
                    logger.warning("Did not add %s as %s is not a valid program"
                                   " ID" % (label, prog['id']))
                if not prog['notify']:
                    logger.debug("Did not add %s as notifyPi is No" % label)
        else:
            # Already exists in DB, check for updates.
            logger.debug("%s is already present, check for updates" % label)
            n = query.first()
            if n.piemail != prog['piEmail']:
                logger.info("Updating PIemail for %s" % label)
                n.piemail = prog['piEmail']
            if n.ngoemail != prog['ngoEmail']:
                logger.info("Updating NGOemail for %s" % label)
                n.ngoemail = prog['ngoEmail']
            if n.csemail != prog['csEmail']:
                logger.info("Updating CSemail for %s" % label)
                n.csemail = prog['csEmail']

            session.commit()
            # If notifyPi is No, delete it from the notification table
            if not prog['notify']:
                logger.info("Deleting %s: notifyPi set to No")
                session.delete(n)
                session.commit()

    logger.info("Processed %s programs" % nprogs)


def update_programs(session, progdicts, logger=DummyLogger()):
    """
    Update programs and obslog comments in the local Fits Server to match
    progdicts, which is a list of dictionaries containing the program info.

    Parameters
    ----------
    session : :class:`sqlalchemy.orm.session.Session`
        SQL Alchemy session to operate in
    progdicts : list of dictionaries
            program information from ODB
    logger: logger-like instance or None
            logger to write messages to

    Returns
    -------
    None
    """
    nprogs = 0
    for prog in progdicts:
        nprogs += 1

        progid = prog['id']
        # Search for this program ID in programs table
        query = session.query(Program).\
            filter(Program.program_id == progid)
        if query.count() == 0:
            # This program doesn't exist in DB yet, add it.
            logger.debug("Adding program %s to programs table", progid)
            prog_obj = Program(prog)
            session.add(prog_obj)
        else:
            # Already exists in DB, check for updates.
            logger.debug("%s already present, updating" % progid)
            p = query.first()
            p.update(prog)

        session.commit()

        # Handle any obslog comments
        ncoms = 0
        if prog.get('obslog_comments') is not None:
            for olc in prog['obslog_comments']:
                ncoms += 1
                # Does it already exist?
                olc_obj = session.query(ObslogComment).\
                    filter(ObslogComment.data_label == olc['label']).first()
                if olc_obj:
                    # Ensure comment us up to date.
                    # Assume program ID doesn't change
                    olc_obj.comment = olc['comment']
                else:
                    # Add a new ObslogComment
                    olc_obj = ObslogComment(olc['label'], olc['comment'])
                    session.add(olc_obj)
            session.commit()
            logger.info("Processed %s ObsLogComments", ncoms)
    logger.info("Processed %s programs", nprogs)
