import pytest

from fits_storage.orm.notification import Notification
from fits_storage.utils.notifications import ingest_odb_xml


@pytest.mark.usefixtures("rollback")
def test_ingest_odb_xml(session):
    session.query(Notification).filter(Notification.selection == 'GN-ENG20000101/science').delete()

    xmlstr = """
    <program>
        <reference>GN-ENG20000101</reference>
        <investigators>
            <investigator pi='true'>
                <name>Foo</name>
                <email>pi@pi.pi</email>
            </investigator>
            <investigator pi='false'>
                <name>Bar</name>
                <email>coi@coi.coi</email>
            </investigator>
        </investigators>
        <ngoEmail>ngo@ngo.ngo</ngoEmail>
        <contactScientistEmail>contact@contact.contact</contactScientistEmail>
        <notifyPi>Yes</notifyPi>
    </program>
    """

    ingest_odb_xml(session, xmlstr)

    n = session.query(Notification).filter(Notification.selection == 'GN-ENG20000101/science').one()

    assert(n.piemail == 'pi@pi.pi')
    assert(n.ngoemail == 'ngo@ngo.ngo')
    assert(n.csemail == 'contact@contact.contact')
