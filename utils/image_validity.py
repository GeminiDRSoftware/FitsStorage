"""
This module provides the image scoring functionality needed by the version
resolver. The rules have been put aside so that they can be reused later
for other purposes.
"""

import bz2
from pyfits import open as pfopen

__all__ = ['score_file']

class ScoringViolation(Exception):
    def __init__(self, message, score = 0):
        super(Exception, self).__init__(message)
        self.score = score

class ScoringResult(object):
    def __init__(self):
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

class Scorer(object):
    __registry = []

    @classmethod
    def register_scoring_rule(cls, rule):
        "Just register this the rule for later use."
        cls.__registry.append(rule)

        return rule

    def score_headers(self, headers):
        score = ScoringResult()

        for rule in self.__registry:
            try:
                score += rule(headers)
            except ScoringViolation as sv:
                score.add(str(sv), sv.value)

        return score

register_rule = Scorer.register_scoring_rule

@register_rule
def score_quality_assessment(headers):
    "RAWGEMQA header - pick the file where != 'UNKNOWN'"
    try:
        if 'UNKNOWN' in headers[0]['RAWGEMQA'].upper():
            return 0

        return 1
    except KeyError:
        raise ScoringViolation("RAWGEMQA not present in the main HDU")

@register_rule
def score_conditions_assessment(headers):
    "RAWIQ, RAWCC, RAWWV, RAWBG headers - pick the one where != 'UNKNOWN'"
    score = 0
    missing = []
    for card in ('RAWIQ', 'RAWCC', 'RAWWV', 'RAWBG'):
        try:
            if 'UNKNOWN' not in headers[0][card].upper():
                score += 1
        except KeyError:
            missing.append(card)

    if missing:
        raise ScoringViolation("Headers {0} not present in the main HDU".format(', '.join(missing)))

    return score

scorer = Scorer()

def score_file(path):
    """Takes the path to a FITS file, or a file-like object, and returns a
       ScoringResult object that describes its validity in terms of the
       registered rules"""

    # Note: (str, unicode) is valid for Python 2; There's no "unicode" type in Python3
    if isinstance(path, (str, unicode)) and ('.bz2' in path):
        fits = bz2.BZ2File(path)
    else:
        fits = path

    headers = [x.header for x in pfopen(fits)]

    return scorer.score_headers(headers)

# Informal testing suite
if __name__ == '__main__':
    import sys

    print ("Testing files provided in the command line...")
    for path in sys.argv[1:]:
        score = score_file(path)
        print("{0}: {1}".format(path, score.value))
        for problem in score.problems:
            print("    {0}".format(problem))
