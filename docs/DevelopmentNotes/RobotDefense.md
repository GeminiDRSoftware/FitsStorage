# Robot Defense system

The public archive sees sporadic but increasing and very intense bursts
of 'spam' queries. These resemble brute force website crawling, and with the 
link-rich content of our search results, this crawling results in massive
numbers of database hits. Most of the queries are light load on the
database, but there can be massive numbers of them. This issue has ramped
up rapidly recently (mid 2024-ish) which I suspect is AI / LLM training.

These are generally distributed (but obviously coordinated), with a 
series of queries coming from multiple IP addresses.

These notes describe how we (plan to) deal with this.

## Internet address allocation terminology reminder (IP4 examples for now)
* an IP address (eg 111.222.123.1) identifies an individual network interface 
on an individual host
* an IP prefix (eg 111.222.123.0/24) identifies a range of IP addresses. The
/24 (in this example) is the netmask and indicates that this prefix contains
all IP address where the first 24 bits of the address match the prefix.
* An autonomous system (AS) is (per wikipedia) a **collection of connected 
Internet Protocol (IP) routing prefixes under the control of one or more 
network operators on behalf of a single administrative entity** or domain, 
that presents a common and clearly defined routing policy to the Internet.
Each AS is assigned an autonomous system number (ASN), for use in Border 
Gateway Protocol (BGP) routing.

So an ASN identifies a set of IP prefixes that are controlled by a single
entity. When we identify a malicious IP address, we certainly want to block
the entire routing prefix that contains it, but also we more than likely want 
to block all the prefixes from the same ASN.

## Mitigation Strategy

We will create and maintain a blocklist in a database table, that lists
blocked IP CIDRs which are the IP allocation prefixes of offending IP
addresses.

Entries are added to the blocklist table from periodic usagelog table
analysis. We do it as log analysis rather than strictly real-time as in most
cases we do not want to block on a single occurrence, rather we detect
multiple malicious requests coming from the same IP prefix. Requests can be
determined to be malicious by sheer number of requests coming from an IP
prefix, but also by analysis of the request (for example multiple
occurrences of an obstype in the URL suggest it comes from a crawler, or
perhaps the referrer or user_agent strings may indicate it is spam).

We will use the api.bgpview.io service to determine the CIDR "prefix" to block
for a given bad actor IP address. It is not currently obvious whether to block
the IP allocation prefix or all ASN prefixes. To avoid repeated queries
to the API, we will cache results from it in a database table and use the
postgres IP address types to determine if an address is in an already
identified CIDR before querying.

bgpview.io seems to go down from time to time, but gives the best info when
it is up. An alternate is to query rdap.arin.net which is the American RIR, but
which seems to respond for allocations from other RIRs anyway. It's not clear
it gives all the prefixes associated with the ASN, but it's a start.

We can keep a "badness score" for each prefix we know about, and when the score
exceed a threshold, we block them. This prevents us blocking legitimate users
with poorly written scripts, for example.


## Blocking options.

Option 1: block them in the AWS VPC ACL is not viable as this is limited to
20 addresses.

Option 2: Run firewalld or similarly on the archive host and feed the prefixes
to a firewalld block list to block them at the kernel IP level
on the archive host. More specifically, firewalld would block an ipset, and we 
would update the IP set from the database table. There are some mildly
irritation permissions issues to solve here, in that generally you need 
superuser permissions to update the firewall, so we'd need to facilitate that
via sudo or setuid, which can end up being a little messy.

Option 3: In the python application or middleware layer, at the start of
handling a request we could check for the ip address in the prefix table and
return http 403 forbidden or similar if it's on the blocklist. There are pros
and cons to this approach compared to a firewall type approach. The con is that
the request makes it through to our application layer and requires a database
lookup to service, even if we're blocking it. If we block them at a firewall, 
the kernel will do that very efficiently at a low level and the application
layer will never see it. The pros are that we get a log of blocked requests and
that we send an explicit forbidden message, both in the text of the response
and in the response code. If we block a legitimate user, it's better if we do
this otherwise they will not be able to tell the difference between getting
blocked and the archive or connection simply being down. Also, this would give
us the option to not block the request if it contains a valid session cookie 
for example, and to not block the login page. Given the diverse sources of
these crawlers, it's only a matter of time until we get a legitimate user on
the same network prefix as a crawler, and simply requiring that user to create
and account and log in the only reasonable solution in that case. It's 
worth bearing in mind that what we're trying to block are not DoS attacks,
they're just annoying web crawlers that don't respect our robots.txt file and
that blindly follow circular links in the results pages. They feed off the 
link-rich nature of our results pages and once we stop feeding them, they'll 
probably go away. If we do get DDoSed, we need to block that at the at a real
firewall level, preferably at AWS, but that's not been an issue (yet).

## Implementation 

We have an ipprefix table, each row corresponds to a CIDR routing prefix.
This contains the prefix details (CIDR), and some metadata that we may have
discovered when we looked it up (eg ASN, name, description, API service we used,
etc), and also has columns for an integer "badness" score, and a boolean
"blocked" flag.

When we service a connection to the web server, we check if it comes from an
IP address that is contained within any blocked prefix. If it is, then we 
require the user to be logged in to service that request. The login and help 
pages at least will need to be an exception to this. If the request comes from
an IP address that is not in a blocked prefix, it is handled as normal.

When we analyse a log entry, we first check if the IP address it comes from is 
contained within any known prefix. If it isn't, we initiate a BGP lookup on the
IP address, and add entries to the prefix table for all the prefixes of that 
ASN. Next, we analyse the URL and other characteristics of the request, and if
we find it to be a malevolent (bad) request, we give it a positive badness 
score. If we find it to be a benevolent (good) request, we give it a negative 
badness score. We then apply that badness adjustment to the badness score of 
all prefixes that come from the same ASN as the request prefix. If we don't know
the ASN, we just apply it to the request prefix.

If the badness score of any prefix exceeds a threshold value, we set
blocked = True for that prefix.