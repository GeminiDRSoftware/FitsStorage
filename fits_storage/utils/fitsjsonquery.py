import requests


class FitsJsonQuery():
    """This class implements a simple cache for querying
    the JSON APIs on FitsStorage servers"""

    baseurl = None
    cachedict = {}

    def __init__(self, baseurl=None, thing=None):
        """baseurl is the base url to query. The class will add
        /filepre=blahblah to this when making the queries.
        thing is the item in the json you are querying. eg data_md5"""
        self.baseurl = baseurl
        self.thing = thing

    def query(self, filename):
        if filename in self.cachedict:
            # Cache Hit
            return self.cachedict.pop(filename)
        else:
            # Cache Miss
            # Get a broad swathe from the server and cache it

            if filename.startswith('N20') or filename.startswith('S20'):
                filepre = filename[:9]
            else:
                filepre = filename
            url = self.baseurl + "/filepre=%s" % filepre
            r = requests.get(url)
            if r.status_code != 200:
                return None
            for f in r.json():
                fn = f['filename']
                if fn.endswith('.bz2'):
                    fn = fn[:-4]
                self.cachedict[fn]=f[self.thing]

            # Hopefully it's a cache hit now
            if filename in self.cachedict:
                return self.cachedict.pop(filename)

            # If not, it wasn't found
            return None
