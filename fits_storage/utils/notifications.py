"""
Notifications utils - add / update notification table entries from ODB XML
"""

from xml.dom.minidom import parseString
from orm.notification import Notification

from gemini_metadata_utils import GeminiProgram

def ingest_odb_xml(session, xml):
    report = []
    nprogs = 0
    dom = parseString(xml)
    for pe in dom.getElementsByTagName("program"):
        readok = True
        try:
            progid = pe.getElementsByTagName("reference")[0].childNodes[0].data
        except:
            report.append("ERROR: Failed to process program node")
            readok = False
        piEmail = ""
        ngoEmail = ""
        csEmail = ""
        # Default notifications off. Should be turned on by xml for valid programs.
        notifyPi = "No"
        nprogs += 1
        # Sometimes people use ;s for separators in the odb email fields...
        try:
            piEmail = pe.getElementsByTagName("piEmail")[0].childNodes[0].data
            piEmail = piEmail.replace(';', ',')
        except:
            pass
        try:
            ngoEmail = pe.getElementsByTagName("ngoEmail")[0].childNodes[0].data
            ngoEmail = ngoEmail.replace(';', ',')
        except:
            pass
        try:
            csEmail = pe.getElementsByTagName("contactScientistEmail")[0].childNodes[0].data
            csEmail = csEmail.replace(';', ',')
        except:
            pass
        try:
            notifyPi = pe.getElementsByTagName("notifyPi")[0].childNodes[0].data
        except:
            pass
    
        if readok:
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
                    n.to = piEmail
                    if len(ngoEmail) == 0:
                        n.cc = csEmail
                    elif len(csEmail) == 0:
                        n.cc = ngoEmail
                    else:
                        n.cc = "%s,%s" % (ngoEmail, csEmail)
                    report.append("Adding notification %s" % label)
                    session.add(n)
                    session.commit()
                else:
                    if gp.valid != True:
                        report.append("Did not add %s as %s is not a valid program ID" % (label, progid))
                    if notifyPi != 'Yes':
                        report.append("Did not add %s as notifyPi is No" % label)
            else:
                # Already exists in DB, check for updates.
                report.append("%s is already present, check for updates" % label)
                n = query.first()
                if n.to != piEmail:
                    report.append("Updating to for %s" % label)
                    n.to = piEmail
                    session.commit()
                if n.cc != "%s,%s" % (ngoEmail, csEmail):
                    report.append("Updating cc for %s" % label)
                    n.cc = "%s,%s" % (ngoEmail, csEmail)
                    session.commit()
                # If notifyPi is No, delete it from the noficiation table
                if notifyPi == 'No':
                    report.append("Deleting %s: notifyPi set to No")
                    session.delete(n)
                    session.commit()
            
    report.append("Processed %s programs" % nprogs)

    return report
