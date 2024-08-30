import pytest
from ipaddress import ip_address, ip_network

from fits_storage_tests.code_tests.helpers import make_empty_pg_testing_db

from fits_storage.config import get_config

from fits_storage.server.prefix_helpers import get_ipprefix
from fits_storage.server.orm.ipprefix import IPPrefix

# These test call the bgpview API. Repeated API calls with the same query
# will get denied by the API.

# In order to actually run this, you need to swap the comment and active lines
# below, and set your local configuration to use a postgres DB, not sqlite
# fsc = get_config(builtinonly=True, reload=True)
fsc = get_config()


@pytest.mark.skipif(fsc.using_sqlite is True,
                    reason='Cannot test IPPrefix functionality using sqlite')
def test_get_ipprefix():
    session = make_empty_pg_testing_db()

    # This is in the UH IP address block
    test_ip = '128.171.1.2'
    test_ipa = ip_address(test_ip)

    ipp = get_ipprefix(session, test_ip, api='bgpview')

    assert ipp is not None
    assert ipp.prefix == '128.171.0.0/16'

    ipn = ip_network(ipp.prefix)
    assert test_ipa in ipn

    assert ipp.asn == 6360

    # Check we got various other top level UH networks. Note, it's possible
    # UH will change things and this will break...

    query = session.query(IPPrefix).filter(IPPrefix.asn == 6360)

    assert query.count() > 10
