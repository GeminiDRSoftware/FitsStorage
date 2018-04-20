"""
This module provides the image scoring functionality needed by the version
resolver. The rules have been put aside so that they can be reused later
for other purposes.

"""
import bz2

from time import strptime
from datetime import datetime
from collections import defaultdict

from astropy.io.fits import open as pfopen
from astropy.io.fits.verify import VerifyError

# ------------------------------------------------------------------------------
NULL_DATETIME = datetime(1, 1, 1, 0, 0, 0)

# ------------------------------------------------------------------------------
__all__ = ['score_file']

# ------------------------------------------------------------------------------
class ScoringViolation(Exception):
    def __init__(self, message, score = 0):
        super(Exception, self).__init__(message)
        self.value = score

class ScoringResult(object):
    def __init__(self, path):
        self.value = 0
        self.problems = []

    def add(self, problem, amount = 0):
        self.problems.append(problem)
        self.value += amount

    def __add__(self, amount):
        s = ScoringResult()
        s.value = self.value + amount
        s.problems = self.problems[:]

        return s

    def __iadd__(self, amount):
        self.value += amount

        return self

def tlm_to_datetime(value):
    for pattern in ('%H:%M:%S (%d/%m/%Y)', '%Y-%m-%dT%H:%M:%S'):
        try:
            dt = datetime(*strptime(value, pattern)[:6])

            return dt
        except ValueError: # Not recognized as a properly formatted IRAF-TLM value
            pass

    return NULL_DATETIME

def header_to_keyword_set(h):
    return set(h.keys() if h is not None else [])

class KeywordSet(object):
    def __init__(self):
        self.hdus = defaultdict(set)
        self._iraftlm = NULL_DATETIME

    @property
    def tlm(self):
        return self._iraftlm

    @tlm.setter
    def tlm(self, value):
        dt = tlm_to_datetime(value)
        if dt > self._iraftlm:
            self._iraftlm = dt

    @property
    def set_list(self):
        return [self.hdus[x] for x in sorted(self.hdus)]

    def add_keywords_from_headers(self, *headers):
        for n, header in enumerate(headers):
            self.hdus[n].update(set(k for k in header.keys() if k))
            try:
                self.tlm = header['IRAF-TLM']
            except KeyError:
                pass

    def __repr__(self):
        return '<KeywordSet {0}>'.format(self.hdus)

    def __sub__(self, other):
        "Returns the keys present in this set, but not in the passed HDUList"
        result = []
        for this, theother in ((x, header_to_keyword_set(y)) for (x, y) in zip(self.set_list, other)):
            result.append(this - theother)

        return result

    def __rsub__(self, other):
        "Returns the keys present in the passed HDUList but not in this set"
        result = []
        for this, theother in ((x, header_to_keyword_set(y)) for (x, y) in zip(self.set_list, other)):
            result.append(theother - this)

        return result

class Scorer(object):
    __registry = []

    def __init__(self):
        self.keywords = KeywordSet()
        self.paths = {}
        self.broken = []

    @classmethod
    def register_scoring_rule(cls, rule):
        "Just register this the rule for later use."
        cls.__registry.append(rule)

        return rule

    def add_path(self, path):
        # Note: (str, unicode) is valid for Python 2; There's no "unicode" type in Python3
        if isinstance(path, (str, unicode)):
            if '.bz2' in path:
                fits = bz2.BZ2File(path)
            else:
                fits = open(path)
        else:
            raise ValueError('{0} is not a string object'.format(path))

        try:
            img = pfopen(fits)
            img.verify('silentfix+exception')
            headers = [x.header for x in img]
            self.paths[path] = headers
            self.keywords.add_keywords_from_headers(*headers)
        except (IOError, ValueError, VerifyError) as e:
            sr = ScoringResult(path)
            sr.add(str(e), -10000)
            self.broken.append((path, sr))

    def compute_scores(self, verbose = False):
        scores = []
        for path, headers in self.paths.items():
            if verbose:
                print("Scoring: {0}".format(path))
            score = ScoringResult(path)

            for rule in self.__registry:
                try:
                    sc = rule(headers, keywords = self.keywords)
                    if verbose:
                        print('{0}: {1}'.format(rule.func_name, sc))
                    score += sc
                except ScoringViolation as sv:
                    score.add(str(sv), sv.value)
                except VerifyError as ve:
                    score.add(str(ve), -10000)

            scores.append((path, score))

        return scores + self.broken

register_rule = Scorer.register_scoring_rule

@register_rule
def score_quality_assessment(headers, *args, **kw):
    "RAWGEMQA header - pick the file where != 'UNKNOWN'"
    try:
        if 'UNKNOWN' in headers[0]['RAWGEMQA'].upper():
            return 0

        return 10
    except KeyError:
        raise ScoringViolation("RAWGEMQA not present in the main HDU")

@register_rule
def score_conditions_assessment(headers, *args, **kw):
    "RAWIQ, RAWCC, RAWWV, RAWBG headers - pick the one where != 'UNKNOWN'"
    score = 0
    missing = []
    for keyword in ('RAWIQ', 'RAWCC', 'RAWWV', 'RAWBG'):
        try:
            if 'UNKNOWN' not in headers[0][keyword].upper():
                score += 10
        except KeyError:
            missing.append(keyword)

    if missing:
        raise ScoringViolation("Headers {0} not present in the main HDU".format(', '.join(missing)))

    return score

# TODO: Have a GOOD look at this. This is pure hunch-driven heuristic. It may be that
#       missing keywords punish the correct version of a given file.
@register_rule
def score_missing_keywords(headers, keywords, *args, **kw):
    "Takes the global keyword set and awards negative points for missing cards"
    return -5 * sum([len(x) for x in keywords - headers])

@register_rule
def score_existing_favorable_keywords(headers, *args, **kw):
    "Score extra points if certain keywords are there"
    score = 0
    if 'RELEASE' in headers[0]:
        score += 20

    return score

@register_rule
def score_standard_correct_keywords(headers, *args, **kw):
    "Look for certain keywords and award points if they follow the FITS standard"
    score = 0
    if type(headers[0].get('EQUINOX')) is float:
        score += 10

    return score

@register_rule
def score_most_recent_iraf_tlm(headers, keywords, *args, **kw):
    "If IRAF-TLM is present, use it as a hint"
    score = 0
    for head in headers:
        try:
            tlm = tlm_to_datetime(head['IRAF-TLM'])
            if tlm >= keywords.tlm:
                score = 5
                break
        except KeyError:
            # It was not there
            pass

    return score

@register_rule
def penalize_handmade_cards(headers, *args, **kw):
    """Some old images have screwed up cards without the mandatory space
       after the name and before =

       This rule penalizes such behaviour..."""

    score = 0
    for head in headers:
        score -= 5 * len(filter(lambda (x,y): isinstance(y, (str, unicode)) and y.startswith("='"), head.iteritems()))

    return score

def score_files(*paths, **kw):
    """Takes the path to a FITS file, or a file-like object, and returns a
       ScoringResult object that describes its validity in terms of the
       registered rules"""

    verbose = (kw['verbose'] if 'verbose' in kw else False)

    scorer = Scorer()
    for p in paths:
        scorer.add_path(p)

    return scorer.compute_scores(verbose)

# Informal testing suite
if __name__ == '__main__':
    import sys

    print ("Testing files provided in the command line...")
    paths = sys.argv[1:]
    if paths:
        scores = score_files(*paths)
        for path, score in scores:
            print("{0}: {1}".format(path, score.value))
            for problem in score.problems:
                print("    {0}".format(problem))
