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


def score_url(url):
    """
    Generate a "badness" score for a URL. 0 is neutral, positive score suggests
    URL may come from a malevolent robot or user. negative score suggests
    benevolent user.

    Multiple obsclasses or obstypes in the URL very much suggest this is
    blind link following (ie a rampaging robot)
    """
    url_words = url.split('/')
    score = 0

    score += _score_obs(url_words, obs_classes)
    score += _score_obs(url_words, obs_types)

    return score
