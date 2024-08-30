"""
This module contains helper code for the archive robot defenses.
"""

from fits_storage.gemini_metadata_utils.telescope_instruments import \
    obs_types, obs_classes


def _score_obs(url_words, obsthings):
    """
    Scoring code that is used for both obsclasses and obstypes
    """
    score = 0

    # We want to detect FLAT/ARC as well as FLAT/FLAT.
    # But FLAT/FLAT is considered doubly bad.
    n_obsthings = 0
    for i in obsthings:
        n = url_words.count(i)
        n_obsthings += n
        # Penalize having the same obsthing multiple times
        if n > 1:
            score += 2 * n

    # Penalize having multiple obsthings. Yes, there's a deliberate double
    # jeopardy going on here.
    if n_obsthings > 1:
        score += n_obsthings

    return score


def score_uri(uri):
    """
    Generate a "badness" score for a URI. 0 is neutral, negative score suggests
    URL may come from a good robot or user. positive score suggests
    bad user.

    Multiple obsclasses or obstypes in the URL very much suggest this is
    blind link following (ie a rampaging robot)
    """
    uri_words = uri.split('/')
    score = 0

    score += _score_obs(uri_words, obs_classes)
    score += _score_obs(uri_words, obs_types)

    return score


def score_referrer(ref):
    """
    Generate a "badness" score for a http referrer. 0 is neutral, positive
    score is bad.

    Not implemented yet. Will likely need to be driven by a lookup, avoid
    hard-coding values here.
    """
    return 0


def score_agent(agent):
    """
        Generate a "badness" score for a http user agent. 0 is neutral, positive
        score is bad.

        Not implemented yet. Will likely need to be driven by a lookup, avoid
        hard-coding values here.
        """
    return 0
