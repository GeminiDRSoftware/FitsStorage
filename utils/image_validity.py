"""
This module provides the image scoring functionality needed by the version
resolver. The rules have been put aside so that they can be reused later
for other purposes.
"""

import bz2
import pyfits
from pyfits import open as pfopen
from collections import defaultdict

__all__ = ['score_file']

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

def header_to_keyword_set(h):
    return set(h.keys() if h is not None else [])

class KeywordSet(object):
    def __init__(self):
        self.hdus = defaultdict(set)

    @property
    def set_list(self):
        return [self.hdus[x] for x in sorted(self.hdus)]

    def add_keywords_from_headers(self, *headers):
        for n, header in enumerate(headers):
            self.hdus[n].update(set(k for k in header.keys() if k))

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
        if isinstance(path, (str, unicode)) and ('.bz2' in path):
            fits = bz2.BZ2File(path)
        else:
            raise ValueError('{0} is not a string object'.format(path))

        try:
            img = pfopen(fits)
            img.verify()
            headers = [x.header for x in img]
            self.paths[path] = headers
            self.keywords.add_keywords_from_headers(*headers)
        except (IOError, pyfits.verify.VerifyError) as e:
            print e
            sr = ScoringResult(path)
            sr.add(str(e), -10000)
            self.broken.append((path, sr))

    def compute_scores(self):
        # TODO: Redo this thing...
        scores = []
        for path, headers in self.paths.items():
            score = ScoringResult(path)

            for rule in self.__registry:
                try:
                    score += rule(headers, keywords = self.keywords)
                except ScoringViolation as sv:
                    score.add(str(sv), sv.value)
                except pyfits.verify.VerifyError as ve:
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

def score_files(*paths):
    """Takes the path to a FITS file, or a file-like object, and returns a
       ScoringResult object that describes its validity in terms of the
       registered rules"""

    scorer = Scorer()
    for p in paths:
        scorer.add_path(p)

    return scorer.compute_scores()

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
