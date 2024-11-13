class Selection(dict):
    """
    This class implements the FitsStorage selection concept.
    This is a dictionary with a few added methods that interpret or render the
    dictionary in various ways.

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._url = None

    from .say_selection import say
    from .to_url import to_url
    from .query_selection import filter
    from .misc import openquery, packdefaults, unpackdefaults
