import requests
import http
import json

from fits_storage.logger import DummyLogger

def get_publications(json_inject=None, logger=DummyLogger()):
    """
    Get the publications from the web server. The Gemini web server provides
    us a JSON document that contains a list of dictionaries, one describing
    each publication in the database. This function fetches that, parses it,
    and does some pre-processing clean-up of the data structure - for example
    converting "Y/N" strings into booleans, and converting the program IDs
    list string for each publication into an actual list.

    If the json_inject argument is passed, it should contain the text of an
    equivalent JSON, primarily for testing purposes
    """

    if json_inject:
        logger.info("Using injected publications database JSON text")
        publications = json.loads(json_inject)
    else:
        # Fetch publications json file from web server. We could make the
        # server URL a configuration item, but given how special-purpose this
        # script is, we don't bother. The URL is public, we don't need any
        # cookies or authentication
        url = 'https://www.gemini.edu/science/publications/publication/allJSON'
        logger.info("Fetching publications from %s", url)
        try:
            r = requests.get(url, timeout=30)
            if r.status_code != http.HTTPStatus.OK:
                logger.error(
                    "Bad HTTP status code %s fetching publication JSON data "
                    "from %s. Exiting.", r.status_code, url)
                exit(1)
            publications = r.json()
        except requests.exceptions.RequestException:
            logger.error(
                "Exception fetching publication JSON data from %s. Exiting",
                url, exc_info=True)
            exit(1)

    logger.info("Got %d publications from web server", len(publications))

    # Loop through them, doing some cleanup
    for pub in publications:
        bibcode = pub.get('bibcode')
        if bibcode is None:
            logger.warning("Got a publication from the web server with no"
                           "bibcode - id %s", pub.get('id'))
            continue
        logger.debug("Pre-processing publication %s", bibcode)

        # Fix up the Y/N columns in the dict to booleans up-front
        for thing in ['gstaff', 'gsa', 'golden', 'too']:
            if pub[thing] == 'Y':
                pub[thing] = True
            elif pub[thing] == 'N':
                pub[thing] = False
            else:
                pub[thing] = None

        # Fix up the program_id string into a programs_ids list
        # The program_id item in the dict is a string which is a comma-space
        # separated list of program_id, with a trailing "\r\n". We first
        # remove the /r/n and all the whitespace, then make a list...
        pid_string = pub.get('program_id')
        if pid_string is not None:
            pid_string = pid_string.removesuffix('\r\n').replace(' ', '')
            pub['program_ids'] = pid_string.split(',')
        else:
            pub['program_ids'] = []
        logger.debug('Program IDs is %s', pub['program_ids'])

    return publications