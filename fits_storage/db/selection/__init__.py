class Selection(object):
    """
    This class implements the FitsStorage selection concept.
    The _seldict dictionary is the fundamental representation of a selection.
    Methods are provided to populate it from, and/or to output it as
    other forms such as URLs, or human-readable strings, and to provide metrics
    of the selection such as whether it's an open query.

    Note, we don't make this class a subclass of dict as it's __init__ function
    takes different arguments to that of dict.

    """
    def __init__(self, thing=None):
        self._seldict = None
        self._url = None

        # The selection can be initialized from a number of different things.
        if isinstance(thing, dict):
            # Being passed a selection dictionary directly
            self._seldict = thing

        if isinstance(thing, list):
            # Being passed a list of "things" parsed from a URL
            self.from_url_things(thing)

        if isinstance(thing, str):
            # Being passed a URL string directly
            things = thing.split('/')
            self.from_url_things(things)

    from .get_selection import from_url_things
    from .say_selection import say
    from .to_url import to_url
    from .query_selection import filterquery
    from .misc import openquery