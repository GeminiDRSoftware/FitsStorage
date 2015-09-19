"""Helper functions for complex database constructs"""

from sqlalchemy import cast, func
from sqlalchemy import BigInteger, Integer

    # This little function we'll use later to cast many of the results into integers. This is mainly
    # to translate booleans (True, False) into numbers, because often a True means '1 of this'. Thus,
    # we can use the result later in sums and products.
def to_int(expr, big=False):
    """Used to cast a result to an integer. This is mainly meant to treat booleans (False and True) as
       numbers (0 and 1), in order to tally results"""
    return cast(expr, Integer if not big else BigInteger)

def null_to_zero(expr):
    """COALESCE(value [, ...]) returns the first of its arguments that is not null. This is useful
       to prevent NULL values breaking aggregate functions, like SUM(...), by providing an integer
       number instead"""
    return func.coalesce(expr, 0)
