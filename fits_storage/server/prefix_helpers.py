"""
This module contains helper code for the archive robot defenses.
"""
import datetime
import time
import requests
from ipaddress import ip_network

from sqlalchemy.exc import IntegrityError

from fits_storage.logger_dummy import DummyLogger

from fits_storage.server.orm.ipprefix import IPPrefix

from fits_storage import utcnow


def get_ipprefix_from_db(session, ip, logger=DummyLogger()):
    """
    Query for and return an IPPrefix instance from the database that contains
    the given ip address. If none found, return None
    """

    # This is the syntax for prefix contains ipaddress. We can pass ipaddress
    # as a string and the DB engine will convert it.
    query = session.query(IPPrefix).filter(IPPrefix.prefix.op('>>')(ip))

    # There ought to only ever be 1 or 0 results here, but I can see how we may
    # end up with that not being the case, so let's handle that reasonably.
    ipp = query.order_by(IPPrefix.id).first()

    if ipp:
        logger.debug("Found Prefix for %s in database: %s", ip, ipp)
    else:
        logger.debug("No prefix for %s found in database", ip)

    return ipp


def get_ipprefix(session, ip, api=None, logger=DummyLogger()):
    """
    Check the database for an IPPrefix that contains the given ip address. If
    one already exists, return it. Otherwise, do a BGP lookup and add all IP
    prefixes for the ASN responsible for the given IP address to the database,
    then re-do the search and return the prefix containing the given ip address.

    The second search should obviously always give a result, but given the 
    wierdness we see in the BGP data sometimes, handle the case where it
    doesn't, by returning None
    """

    ipp = get_ipprefix_from_db(session, ip, logger=logger)
    if ipp is not None:
        return ipp

    ipps = get_prefixes(ip, api=api, logger=logger)
    for ipp in ipps:
        logger.debug("Adding prefix to database: %s", ipp)
        try:
            session.add(ipp)
            session.commit()
        except IntegrityError:
            logger.warning("Duplicate entry adding ippprefix %s.", ipp)
            session.rollback()

    return get_ipprefix_from_db(session, ip, logger=logger)


def get_prefixes(ip, api=None, logger=DummyLogger()):
    """
    Available apis are 'bgpview' (preferred) or 'arin' (fallback).
    Pass api = 'bgpview' or 'arin' to only call that api, None (default)
    or any other value will try bgpview first then fall back to arin
    """

    bgpview = get_bgpviewapi(logger=logger)
    arin = get_arinapi(logger=logger)

    if api == 'bgpview':
        apis = [bgpview]
    elif api == 'arin':
        apis = [arin]
    else:
        apis = [bgpview, arin]

    for theapi in apis:
        prefixes = theapi(ip)
        if prefixes is not None:
            return prefixes
    return None


# The two "BGP" APIs (BgpViewApi and ArinApi) are instantiated with an optional
# logger argument, then called with an IP address, and return a list of
# IPPrefix instances to add to the database.

# The bgpview API at least implements rate limiting on the server side, so we
# need to keep track of when the last call was. Thus, these should be treated
# as singletons, and these module-level helper functions implement that.
_BGPVIEWAPI = None
_ARINAPI = None


def get_bgpviewapi(broad=True, logger=DummyLogger()):
    global _BGPVIEWAPI
    if _BGPVIEWAPI is None:
        _BGPVIEWAPI = BgpViewApi(broad=broad, logger=logger)
    return _BGPVIEWAPI


def get_arinapi(logger=DummyLogger()):
    global _ARINAPI
    if _ARINAPI is None:
        _ARINAPI = ArinApi(logger=logger)
    return _ARINAPI


class BgpViewApi(object):
    """
    Helper class for calling the bgpview.io APIs
    """
    def __init__(self, broad=True, logger=DummyLogger()):
        self.logger = logger
        self.urlbase = 'https://api.bgpview.io/'
        self.lastcall = utcnow()
        self.ratelimit_secs = 1.0

        # The lastcall value is used to rate limit calls to the API

        # The "broad" flag says whether to capture the "broad" or "narrow"
        # prefixes from the ASN. For example UH lists 121.178.0.0/16 but then
        # also lists lots of 128.171.xxx.0/24 prefixes. If we capture them all
        # then we get lots of duplication, which we don't want. So setting
        # braod=True will capture the /16 wheras broad=False will capture
        # all the separate /24s.
        self.broad = broad

    def __call__(self, ip):
        return self.get_prefixes_from_ip(ip)

    def ratelimit(self):
        t = utcnow() - self.lastcall
        if t.total_seconds() < self.ratelimit_secs:
            self.logger.debug("Rate limiting call to BgpView API...")
            time.sleep(self.ratelimit_secs)
        self.lastcall = utcnow()

    def getjson(self, url):
        self.ratelimit()
        self.logger.debug("Getting %s", url)
        r = requests.get(url)
        if r.status_code == 429:
            # Too many tries
            self.logger.error("HTTP 429 - Too Many Requests from bgpview")
            self.logger.debug("Headers were: %s", r.headers)
            self.ratelimit_secs += 2
            return None
        elif r.status_code != 200:
            self.logger.error("Bad status code %d from %s", r.status_code, url)
            return None
        try:
            j = r.json()
            if j['status'] != 'ok':
                self.logger.error("Status not OK in API JSON: %s - %s",
                                  j['status'], j['status_message'])
                return None
        except Exception:
            self.logger.error("Exception in api getjson", exc_info=True)
            return None

        return j

    def get_asn_from_ip(self, ip):
        """
        Lookup the ASN associated with an IP address
        pass ip address as a string
        returns the ASN of the most specific prefix, or None if failure
        """

        url = self.urlbase + 'ip/' + ip
        j = self.getjson(url)

        # We get a "chain" of ASNs in the response - e.g. the company to which
        # the IP address belongs, and then the ISP of that company, and the
        # ISP of that ISP, and so on.
        # Find the ASN with the largest (most specific) cidr (netmask)
        # Make a dict where the key is the cidr and the value is the ASN
        asns = {}
        try:
            for p in j['data']['prefixes']:
                asns[p['cidr']] = p['asn']['asn']
        except TypeError:
            self.logger.error("Exception extracting ASNs", exc_info=True)

        asn = asns[max(asns.keys())]
        self.logger.debug("Got ASN %s for ip %s", asn, ip)

        return asn

    def get_prefixes_from_asn(self, asn):
        """
        Lookup the ASN and return a list of IPPrefix instances
        """
        url = self.urlbase + 'asn/' + str(asn) + '/prefixes'
        j = self.getjson(url)

        # OK, this is a bit messy. See the note on the broad flag in init.
        # Some parent entries in the json don't seem to make sense, ie the
        # parent stated is not a supernet of the prefix in question.
        # Make a dict where the prefix as an ip_network instance is the key,
        # and the value is a dict of metadata items we'll need later
        ipnets = {}
        for p in j['data']['ipv4_prefixes']:
            try:
                ipn = ip_network(p['prefix'])
                # Validate the parent prefix. We see invalid ones, and they
                # cannot be stored in the database.
                try:
                    pp = p['parent']['prefix']
                    ip_network(pp)
                except ValueError:
                    self.logger.debug("Ignoring invalid parent prefix: %s",
                                      p['parent']['prefix'])
                    pp = None
                meta = {'name': p['name'],
                        'description': p['description'],
                        'parent': pp}
                ipnets[ipn] = meta
            except KeyError:
                self.logger.error("Error processing prefix %s", p)
                continue
            except ValueError:
                self.logger.error("Invalid prefix, ignoring: %s", p)
                continue
        self.logger.debug("Got %d prefixes to process", len(ipnets))

        # Make lists for each prefixlen
        bypl = {}
        for ipnet in ipnets.keys():
            pl = ipnet.prefixlen
            if pl in bypl.keys():
                bypl[pl].append(ipnet)
            else:
                bypl[pl] = [ipnet]

        # "Sort" the ipnets into supernet and subnet sets.
        # A "supernet" is an ipnet that is not a subnet of any other ipnet
        # A "subnet" is an ipnet that is a subnet of another ipnet
        # It is possible that len(supernets) + len(subnets) < len(ipnets)
        # as there could be "intermediate" ipnets, that we're not interested in

        supernets = set()
        subnets = set()

        # Get a sorted list of the prefix lengths in play
        prefixlens = list(bypl.keys())
        prefixlens.sort()

        # The ones with the shortest prefixlengths must be supernets.
        shortest = prefixlens.pop(0)
        for i in bypl[shortest]:
            supernets.add(i)

        # Now, go through the other prefix lengths and put them in the sets
        for pl in prefixlens:
            for ipnet in bypl[pl]:
                for supernet in supernets:
                    if ipnet.subnet_of(supernet):
                        subnets.add(ipnet)
                        break
                else:
                    # Not a subnet of any a known supernet
                    supernets.add(ipnet)

        self.logger.debug("Found %d supernet and %d subnet prefixes",
                          len(supernets), len(subnets))
        # self.logger.debug("Supernets: %s", supernets)
        # self.logger.debug("Subnets: %s", subnets)

        # If we are in "broad" mode, return the parents, if we are in "narrow"
        # mode, return the children
        if self.broad:
            prefixes = supernets
        else:
            prefixes = subnets

        ipprefixes = []
        for prefix in prefixes:
            self.logger.debug("Returning prefix %s", prefix)
            ipp = IPPrefix()
            ipp.prefix = str(prefix)
            ipp.api_used = self.urlbase
            ipp.api_query_utc = utcnow()
            ipp.asn = asn
            ipp.name = ipnets[prefix]['name']
            ipp.description = ipnets[prefix]['description']
            ipp.parent = ipnets[prefix]['parent']

            ipprefixes.append(ipp)

        return ipprefixes

    def get_prefixes_from_ip(self, ip):
        """
        Get all the prefixes associated with an IP address
        """
        asn = self.get_asn_from_ip(ip)

        if asn is None:
            return None

        return self.get_prefixes_from_asn(asn)


class ArinApi(object):
    def __init__(self, logger=DummyLogger()):
        self.logger = logger
        self.urlbase = 'https://rdap.arin.net/registry/ip/'

    def __call__(self, ip):
        return self.get_prefixes_from_ip(ip)

    def get_prefixes_from_ip(self, ip):
        """
        for arin we get a single value direct from an IP lookup, which isn't
        as good but better than nothing.
        """
        url = self.urlbase + ip
        try:
            self.logger.debug("Getting %s", url)
            r = requests.get(url)
            if r.status_code != 200:
                self.logger.error("Bad status code %s from %s",
                                  r.status_code, url)
                return []
            j = r.json()
            v4prefix = j['cidr0_cidrs'][0]['v4prefix']
            length = j['cidr0_cidrs'][0]['length']
            cidr = f"{v4prefix}/{length}"

            if cidr == '0.0.0.0/0':
                return []
            
            ipp = IPPrefix()
            ipp.prefix = cidr
            ipp.api_used = self.urlbase
            ipp.api_query_utc = utcnow()
            ipp.name = j['name']
            ipp.description = j['handle']

            return [ipp]

        except Exception:
            self.logger.error("Exception handling arin data.", exc_info=True)
            return None
