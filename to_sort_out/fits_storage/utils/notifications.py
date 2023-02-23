"""
Notifications utils - add / update notification table entries from ODB XML
"""

from xml.dom.minidom import parseString
from ..orm.notification import Notification
from . import programs

from gemini_obs_db.utils.gemini_metadata_utils import GeminiProgram


def ingest_odb_xml(session, xml):
    """
    Read ODB XML and update notification settings in the Fits Server

    Parameters
    ----------
    session : :class:`sqlalchemy.orm.session.Session`
        SQL Alchemy session to operate in
    xml : str
        XML data from ODB

    Returns
    -------
    array of str : List of text messages describing the changes that were applied
    """
    report = []
    nprogs = 0
    dom = parseString(xml)
    for pe in programs.get_programs(dom):
        nprogs += 1
        try:
            progid = pe.get_reference()
        except IndexError:
            report.append("ERROR: Failed to process program node")
            continue

        _, piEmail = pe.get_investigators()
        ngoEmail = pe.get_ngo_email()
        csEmail = pe.get_contact()
        # Default notifications off. Should be turned on by xml for valid programs.
        notifyPi = pe.get_notify()

        # Search for this program ID in notification table
        label = "Auto - %s" % progid
        query = session.query(Notification).filter(Notification.label == label)
        if query.count() == 0:
            # This notification doesn't exist in DB yet.
            # Only add it if notifyPi is Yes and it's a valid program ID
            gp = GeminiProgram(progid)
            if notifyPi == 'Yes' and gp.valid:
                n = Notification(label)
                n.selection = "%s/science" % progid
                n.piemail = piEmail
                n.ngoemail = ngoEmail
                n.csemail = csEmail
                report.append("Adding notification %s" % label)
                session.add(n)
                session.commit()
            else:
                if not gp.valid:
                    report.append("Did not add %s as %s is not a valid program ID" % (label, progid))
                if notifyPi != 'Yes':
                    report.append("Did not add %s as notifyPi is No" % label)
        else:
            # Already exists in DB, check for updates.
            report.append("%s is already present, check for updates" % label)
            n = query.first()
            if n.piemail != piEmail:
                report.append("Updating PIemail for %s" % label)
                n.piemail = piEmail
            if n.ngoemail != ngoEmail:
                report.append("Updating NGOemail for %s" % label)
                n.ngoemail = ngoEmail
            if n.csemail != csEmail:
                report.append("Updating CSemail for %s" % label)
                n.csemail = csEmail

            session.commit()
            # If notifyPi is No, delete it from the noficiation table
            if notifyPi == 'No':
                report.append("Deleting %s: notifyPi set to No")
                session.delete(n)
                session.commit()

    report.append("Processed %s programs" % nprogs)

    return report
