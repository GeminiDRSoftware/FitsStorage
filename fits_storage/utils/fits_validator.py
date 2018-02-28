#!/usr/bin/env python

from __future__ import print_function

from functools import partial

__all__ = ['RuleSetFactory', 'RuleSet', 'RuleCollection', 'Environment', 'ValidationError', 'BadData',
           'EngineeringImage', 'GeneralError', 'NoDateError', 'Evaluator',
           'STATUSES', 'Result']

"""
This module contains all the machinery for metadata testing. The easiest way to
use it is to import Evaluator, instanciate it and then pass call the evaluator
object passing an HDUList.

  >>> evaluate = Evaluator()
  >>> evaluate(fits)

The result is a named tuple with elements:

  `passes`:  boolean, passes the test or not
  `code`:    a finer grain veredict on the test: CORRECT, ENG, BAD for data
             that can be ingested to the archive; and  NOPASS, EXCEPTION
             for data that should be reviewed
  `message`: human readable message explaining the analysis
"""

import os
import re
from collections import namedtuple, defaultdict
from datetime import datetime
from time import strptime

from ..fits_storage_config import validation_def_path

import yaml
import astropy.io.fits as pf

import logging

# General Exceptions

class ValidationError(Exception):
    pass

# TODO: Explain the use for these exceptions

class BadData(ValidationError):
    pass

class EngineeringImage(ValidationError):
    pass

class GeneralError(ValidationError):
    pass

class NoDateError(Exception):
    pass

# Constants
NOT_FOUND_MESSAGE = "Could not find a validating set of rules"
STATUSES = ['CORRECT', 'NOPASS', 'NODATE', 'BAD', 'ENG', 'EXCEPTION']

fitsTypes = {
    'char': str,
    'float': float,
    'int': int,
    'bool': bool,
    'date': datetime,
    'sexagesimal': str,
    }

rangePatterns = (
    re.compile(r'(\S+)\s+\.\.\s+(\S+)'),
    )

radecPattern = '^[-+]?\d{1,2}:\d{2}:\d{2}(?:\.\d+)?$'

typeCoercion = (
    int,
    float,
    lambda x: datetime(*strptime(x, "%Y-%m-%d")[:6]),
    lambda x: datetime(*strptime(x, "%Y-%b-%d")[:6]),
    lambda x: datetime(*strptime(x, "%Y-%m-%d %H:%M:%S")[:6]),
    lambda x: datetime(*strptime(x, "%Y-%m-%d (%H:%M:%S)")[:6]),
    lambda x: datetime(*strptime(x, "%Y-%m-%dT%H:%M:%S")[:6]),
    )

Function = namedtuple('Function', ['name', 'code', 'exceptionIfTrue', 'exceptionIfFalse'])

compatible_types = {
    float: (float, int),
    }

###############################################################################################
#
# Ancillary functions

# This function will be used in a few places where the default behaviour
# is to return the same value that has been passed, unchanged
identity_lambda = lambda x: x
identity_lambda.__doc__ = """This function takes a single argument an returns it unchanged.
It is meant to be used as a placeholder for transforms"""

def coerceValue(val):
    """Takes a string and tries to convert it to some known value type using the
       functions provided by typeCoercion"""
    for fn in typeCoercion:
        try:
            return fn(val)
        except ValueError:
            pass

    raise ValueError('{0} not a known FITS value'.format(val))

def log(text):
    logging.debug(text)

def get_full_path(filename):
    "Takes a relative path to a rules file and returns the full path (plus extension)"
    return os.path.join(validation_def_path, filename + '.def')

def iter_list(lst):
    """Generator that yields values from an iterable.

    If the value that is passed is not a list or a tuple, it will be returned as-is, effectively
    turning it in a single-element list. This is useful in cases where we find a single value in
    a definition file where we would expect a collection. The only exception is `None`, which won't
    be yielded, making it an *empty* list"""
    if isinstance(lst, (list, tuple)):
        for k in lst:
            yield k
    elif lst is not None:
        yield lst

def iter_pairs(lst, coercion = identity_lambda):
    """Generator that takes a list of elements and a coercion function, and returns them as pairs,
       with the following pattern:

      - if the element is a dictionary, every entrie on it will be yielded as (key, coercion(value))
        pairs
      - if the element is a list or tuple, then the first element is returned as the first element
        of the pair, and the rest as the second, ie. (element[0], coercion(element[1:]))
      - if the element doesn't fall in the two previous cases and is *not* `None`, (element, ()) will
        be returned.

      `None` elements won't be yielded. The default `coercion` is just an identity function.
    """
    for k in iter_list(lst):
        if isinstance(k, dict):
            for key, value in k.items():
                yield key, coercion(value)
        elif isinstance(k, (list, tuple)):
            yield (k[0], coercion(k[1:]))
        elif k is not None:
            yield (k, ())

def iter_keywords(lst):
    """Generator that iterates over a list of keyword definitions and yields pairs (k, v), where k is
       the name of the keyword and v is an instance of KeywordDescriptor that describes the restrictions
       for the keyword"""
    for (key, value) in iter_pairs(lst):
        if key.startswith('if ') or key.startswith('since ') or key.startswith('until ') or key == 'optional':
            if not value:
                raise ValueError('Found "{0}" with no keywords associated'.format(key))

            for k, descriptor in iter_keywords(value):
                descriptor.addRestriction(key)

                yield k, descriptor
            continue

        elif ',' in key:
            splitkey = [x.strip() for x in key.split(',')]
        else:
            splitkey = [key]

        for k in splitkey:
            yield k, KeywordDescriptor(value)

def not_implemented(fn):
    "Decorator for not implemented tests"
    def wrapper(self, *args, **kw):
        raise NotImplementedError("{0}.{1}".format(self.__class__.__name__,
                                                                    fn.func_name))
    return wrapper

def getEnvDate(env):
    for feat in env.features:
        if feat.startswith('date:'):
            return coerceValue(feat[5:])

    raise NoDateError()

####################################################################################
#
# Range testing classes.
#
# The use case for the range testing is to find if a certain value is within certain
# parameters.
#
# Any instance of a class that defines __contains__ can be used as a range test

class CompositeRange(object):
    "This class is used to compose a number of range tests together"
    def __init__(self):
        self.tests = []

    def __contains__(self, value):
        """Returns True if the passed value falls within any of the ranges
           contained by this composite, or if no test has been added to the
           composite"""
        if not self.tests:
            # No ranges defined
            return True

        return any((value in test) for test in self.tests)

    def __repr__(self):
        return '[' + ', '.join(repr(x) for x in self.tests) + ']'

    def append(self, test):
        "Adds an individual test to the composite"
        self.tests.append(test)

class Pattern(object):
    "Range test class that tries matching strings against a regex"
    def __init__(self, pattern):
        self.cpat = re.compile(pattern)

    def __contains__(self, x):
        return self.cpat.match(x) is not None

    def __repr__(self):
        return '<pattern({0!r})>'.format(self.cpat.pattern)

class ArbitraryRangeTest(object):
    """Range test class that will test values against an arbitrary function
       passed by the user. This class exists only so that such functions can
       be added to a CompositeRange"""
    def __init__(self, test, name):
        self.test = test
        self.name = name

    def __contains__(self, x):
        return self.test(x)

class TransformedStringRangeTest(object):
    """Range test class that applies a transform to the input data before
       testing it against a range"""
    def __init__(self, testfn, range):
        """`testfn` is a function that accepts a single value, and returns a
           potentially transformed version of it. This transformed value is
           then tested against `range`"""
        self.testfn = testfn
        self.range = range

    def __contains__(self, x):
        # TODO: This makes any non-string to fail the test, but it seems arbitrary.
        #       Analyze the use cases and see if we really need this
        return isinstance(x, (str, unicode)) and self.testfn(x) in self.range

class Range(object):
    """Range testing class. Instances of this class check if a certain value is
       within the limits of a range of values of integer, floating point, or
       datetime types. """
    def __init__(self, low, high, type):
        self.low, self.high, self.type = low, high, type

    @staticmethod
    def from_string(string, forceType = None):
        """Factory method: takes a string of format 'm .. n' and returns an instance
           of Range that tests if a value belongs to the closed interval [m, n]"""
        for ptrn in rangePatterns:
            try:
                l, h = ptrn.match(string).groups()
                break
            except AttributeError:
                pass
        else:
            raise ValueError("Can't recognize '{0}' as a range".format(string))

        args = dict(low = None, high = None, type = forceType)
        for attr, value in [('low', l), ('high', h)]:
            if value != '*':
                for fn in typeCoercion:
                    try:
                        val = fn(value)
                        args[attr] = val
                        tp = type(val)
                        if args['type'] is None:
                            args['type'] = tp
                        elif tp not in compatible_types.get(args['type'], (args['type'],)):
                            raise TypeError("Two types for a range: {0}".format(string))
                        break
                    except ValueError:
                        pass
                else:
                    # The default type would be char
                    if args['type'] and args['type'] != str:
                        raise TypeError("Two types for a range: {0}".format(string))
                    args[attr] = value
                    args['type'] = str

        return Range(**args)

    @staticmethod
    def from_type(type):
        return Range(low = None, high = None, type = type)

    def __contains__(self, x):
        "Implements the membership test operator, allowing `x in range`"
        if self.type is None:
            return True

        # Try to coerce string types into other...
        if type(x) is str and self.type is not str:
            for fn in typeCoercion:
                try:
                    x = fn(x)
                    break
                except ValueError:
                    pass

        # Special case: not null strings
        if self.low == '' and self.high == '':
            return x not in (None, '')

        return ((type(x) == self.type) and
                ((self.low is None) or (self.low <= x)) and
                ((self.high is None) or (self.high >= x)))

    def __str__(self):
        return "{0} .. {1}".format((str(self.low) if self.low else '*'),
                                   (str(self.high) if self.high else '*'))

def not_null_test(x):
    return isinstance(x, (str, unicode)) and x != ''

NotNull = ArbitraryRangeTest(not_null_test, 'not null')
NotNull.__doc__ = "This test will return True for string values that are non empty"

######################################################################################
#
# Header Test Classes.
#
# The use case for the header tests is to check if a certain header fits within a
# certain environment. The environment can be the general one for the test, one
# defined by the test internal data, or a combination of both.
#
# The header test interface consists on two methods:
#
#   test(self, header, env)
#   __call__(self, header, env)
#
# In general, `__call__` should just invoke `test`

class NegatedTest(object):
    "A simple wrapping class that negates the result of another header test"
    def __init__(self, test, pass_name=False):
        self._negated = test
        self.name = self._negated.name
        if not pass_name:
            self.name = 'not ' + self.name

    def test(self, header, env):
        return not self._negated(header, env)

    def __call__(self, header, env):
        return self.test(header, env)

class AndTest(list):
    """A composite that takes a number of header tests and returns True only
       if all of them pass

       AndTest derives from list, and thus it can be initialized like a list,
       and all its methods apply (append, __getitem__, __delitem__, etc.)"""
    def __init__(self, name=None, default_result=True, *args, **kw):
        """Initialize the list of tests. It can optional accept a `name` and
           a boolean `default_result`. The default result (True if not specified)
           will be return value if the list of tests is empty"""
        super(AndTest, self).__init__(*args, **kw)
        self.name = name
        self._def = default_result

    def test(self, header, env):
        if len(self) == 0:
            return self._def

        return all(test(header, env) for test in self)

    def __call__(self, header, env):
        return self.test(header, env)

class FunctionTest(object):
    """Header test functions are only passed the header to be tested, and the
       current environment. Some test functions may need extra parameters, though.

       This class offers a wrapper that will hold such extra parameters and run
       the function as fn(header, env, *args, **kw)"""
    def __init__(self, func, name, *args, **kw):
        self.fn   = func
        self.args = args
        self.kw   = kw
        self.name = name

    def test(self, header, env):
        return self.fn(header, env, *self.args, **self.kw)

    def __call__(self, header, env):
        return self.test(header, env)

class TestSince(object):
    """Renders the `since` restriction.

       This test is designed to be used when figuring out if a keyword must be
       *ignored*, and thus it returns `True` if the date is *prior* to the `since`
       value"""
    def __init__(self, value):
        self.v = value
        self.name = 'since'

    def test(self, header, env):
        return getEnvDate(env) < self.v

    def __call__(self, header, env):
        return self.test(header, env)

class TestUntil(object):
    """Renders the `until` restriction.

       This test is designed to be used when figuring out if a keyword must be
       *ignored*, and thus it returns `True` if the date *follows* the `until`
       value"""
    def __init__(self, value):
        self.v = value
        self.name = 'until'

    def test(self, header, env):
        return getEnvDate(env) > self.v

    def __call__(self, header, env):
        return self.test(header, env)

class TestEnvCondition(object):
    "This test checks whether a certain value is defined in the current environment features"
    def __init__(self, value, name):
        self.v = value
        self.name = name

    def test(self, header, env):
        return self.v in env.features

    def __call__(self, header, env):
        return self.test(header, env)

def testEnvCondition(value, name, negated):
    "Factory for TestEnvCondition instances that simplifies creating negated ones"
    t = TestEnvCondition(value, name)
    if negated:
        return NegatedTest(t, pass_name=True)
    return t

### End functions for kw descriptor applicability test

class KeywordDescriptor(object):
    """Instances of this class are representations of a keyword descriptor from a
       rules file, like:

         - EXPTIME:
           - float:      0 .. *

         - FILTER2:
           - char:       Open
           - pattern:    '.*G\d{4}'
           - pattern:    '[LHJKx]_\(order_\d\)'

         - RAWCC:
           - char:       Any
           - upper:      UNKNOWN
           - pattern:    '\d{2}-percentile'
    """

    def __init__(self, info):
        """Takes a list of restrictions or modifiers that apply to a keyword,
           interprets them, and stores them as tests that will be applied to
           the keyword's value.

           Valid restrictions are:

             - the type for the keyword (`bool`, `char`, `date`, `int`, `float`)
               and, optionally, it's range (where it applies)
             - `pattern`: regex patterns to be matched against char values
             - `upper`: same as `char`, but converts the whole input to uppercase
                        before comparing
             - `lower`: same as `upper`, but converting to lowercase

           There can be multiple restrictions, and same types can be repeated
           for different ranges. The restrictions will be treated as mutually
           exclusive, ie. if one of them passes the whole test passes.

           Modifiers:

             - `optional: the keyword is not mandatory; if it's missing there will
                          be no error, but if it's present, it has to follow the
                          rules
             - `since`: if the image is older than this date, the restriction
                        test will be skipped completely
        """
        self.reqs = AndTest(default_result = False)
        self.transforms = []
        self.range = CompositeRange()
        self.optional = False
        self.fn = identity_lambda

        # Maybe we should warn when this is None...
        if info is not None:
            for restriction in info:
                self.addRestriction(restriction)

    def addRestriction(self, restriction):
        """Takes the raw representation of a restriction, identifies it and adds the
           appropriate test to the descriptor.

           Will raise `ValueError` if it cannot figure out a valid restriction from the text"""
        if isinstance(restriction, (str, unicode)):
            if restriction in fitsTypes:
                if restriction == 'sexagesimal':
                    self.addRange(Pattern(radecPattern))
                else:
                    self.addRange(Range.from_type(fitsTypes[restriction]))
            elif restriction == 'optional':
                self.optional = True
            elif restriction.startswith('if '):
                value = restriction[3:].strip()
                if value.startswith('not '):
                    fn = testEnvCondition(value[4:], restriction, negated=False)
                else:
                    fn = testEnvCondition(value, restriction, negated=True)
                self.reqs.append(fn)
            elif restriction.startswith('since '):
                coerced = coerceValue(restriction[5:].strip())
                if not isinstance(coerced, datetime):
                    raise ValueError("Wrong value for 'since': {0}".format(value))
                self.reqs.append(TestSince(coerced))
            elif restriction.startswith('until '):
                coerced = coerceValue(restriction[5:].strip())
                if not isinstance(coerced, datetime):
                    raise ValueError("Wrong value for 'until': {0}".format(value))
                self.reqs.append(TestUntil(coerced))
            else:
                raise ValueError("Unknown descriptor {0}".format(restriction))
        if isinstance(restriction, dict):
            kw, value = restriction.items()[0]
            if kw in fitsTypes:
                if kw == 'char':
                    if isinstance(value, str) and ' '.join(value.lower().split()) == 'not null':
                        self.addRange(NotNull)
                    else:
                        self.addRange(set(iter_list(value)))
                elif ' .. ' in value:
                    self.addRange(Range.from_string(value, forceType = fitsTypes[kw]))
                else:
                    self.addRange(set(iter_list(value)))
            elif kw == 'upper':
                self.addTransformedStringRangeTest(str.upper, value)
            elif kw == 'lower':
                self.addTransformedStringRangeTest(str.lower, value)
            elif kw == 'pattern':
                self.addRange(Pattern(value))
            else:
                raise ValueError("Unknown descriptor {0}".format(restriction))

    def addRange(self, rng):
        """Adds a range restriction. `rng` should be an instance of a class that complies
           to the Range Test interface"""
        self.range.append(rng)

    def addTransformedStringRangeTest(self, fn, value):
        """Takes a transform function (`fn`) and a set of restrictions, and builds
           a TransformedStringRangeTest out of them, that will then be added to the
           restrictions"""
        self.addRange(TransformedStringRangeTest(fn, set(iter_list(value))))

    @property
    def mandatory(self):
        """True if the represented keyword is not optional"""
        return not self.optional

    def ignore(self, header, env):
        """Returns True if this descriptor should be ignored for the current header,
           in the current environment"""
        return self.reqs.test(header, env)

    def test(self, value):
        "Returns True if the passed value is a valid one according to the descriptor"
        for fn in self.transforms:
            value = fn(value)

        return value in self.range

reserved_unit_identifiers = {
    'conditions',
    'keywords',
    'provides',
    'range limits',
    'merge',
    'maybe-merge',
    'tests',
}

reserved_global_identifiers = reserved_unit_identifiers | {
    'import',
    'one of',
    'validation',
    'validation(final)',
}

class Environment(dict):
    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(self.__class__.__name__, attr))

    def __setattr__(self, attr, value):
        self[attr] = value

        return value

class OverrideStack(object):
    "Structure to keep keyword override levels"
    def __init__(self):
        self._stack = []

    def push(self, keys):
        self._stack.append(keys)

    def pop(self):
        return self._stack.pop()

    def __contains__(self, value):
        return any(value in level for level in self._stack)

# Extra functions to help RuleSet conditions
def hdu_in(hdus, h, env):
    return env.hduNum in hdus

def run_registered_function(func, header, env):
    """This is a header test that wraps a registered function and takes care
       of raising exceptions, if needed"""
    res = func.code(header, env)
    if isinstance(res, tuple):
        res, message = res
    else:
        message = ''
    if res and func.exceptionIfTrue is not None:
        raise func.exceptionIfTrue(message)
    elif not res and func.exceptionIfFalse is not None:
        raise func.exceptionIfFalse(message)

    return res

def kw_matches_value(kw, value, header, env):
    """Helper function to test keyword in a header against a literal value or
       a range, without further complicating the code in the place of the test"""
    value_to_test = header.get(kw)

    if isinstance(value, (str, unicode)):
        return value_to_test == value

    # If value is not a string, assume that it is an iterable
    return value_to_test in value

def kw_in_header(kw, header, env):
    return kw in header

def named_function(fn, name):
    "Wrapper that adds a name to a object (possibly a test funcion) and then just returns it"
    fn.name = name
    return fn

def get_from_scope(sourcename, scope, name):
    components = name.split('.')
    if len(components) > 2:
        raise RuntimeError("Syntax Error ({0}): {1!r} is invalid, only two scoping levels allowed".format(sourcename, name))
    try:
        first = components.pop(0)
        ret = scope[first]
    except KeyError:
        raise RuntimeError("In {0}: Unknown {1!r}".format(sourcename, first))

    try:
        ret = ret.units[components.pop(0)]
    except KeyError:
        raise RuntimeError("In {0}: Unknown {1!r}".format(sourcename, name))
    except IndexError:
        # There was not a second member
        pass

    return ret

class RuleSetFactory(object):
    __registry = {}

    @classmethod
    def register_function(cls, name, excIfTrue = None, excIfFalse = None):
        def reg(fn):
            cls.__registry[name] = Function(name = name, code = fn,
                                            exceptionIfTrue = excIfTrue,
                                            exceptionIfFalse = excIfFalse)

            return fn
        return reg

    def __init__(self, ruleSetClass):
        self._cls = ruleSetClass
        self._cache = {}

    def __parse_tests(self, sourcename, data, name=None):
        result = AndTest(name=name)

        for entry in data:
            if isinstance(entry, (str, unicode)):
                element, content = entry, []
            elif isinstance(entry, dict):
                element, content = entry.items()[0]
            else:
                raise RuntimeError("Syntax Error: Invalid entry, {0!r} (on {1})".format(entry, sourcename))

            negated = False
            if element.startswith('is '):
                if element.startswith('is not '):
                    negated = True
                    _element = element[6:].strip()
                else:
                    _element = element[2:].strip()

                test_to_add = testEnvCondition(_element, name = element, negated = negated)

            else:
                if element.startswith('not '):
                    negated = True
                    _element = element[3:].strip()
                else:
                    negated = False
                    _element = element

# If we want this, it should go at the validation-unit level
#                if _element == 'on hdus':
#                    test_to_add = FunctionTest(partial(hdu_in, set(iter_list(content))), name = _element)
                if _element == 'exists':
                    test_to_add = AndTest(name = _element)
                    for kw in iter_list(content):
                        test_to_add.append(named_function(partial(kw_in_header, kw),
                                                          name=element))
                elif _element in {'matching', 'matching(pdu)'}:
                    mtest = AndTest(name = _element)
                    for kw, val in iter_pairs(content):
                        mtest.append(named_function(partial(kw_matches_value, kw, val),
                                                    name = '{} {}'.format(element, val)))
                    if _element == 'matching(pdu)':
                        test_to_add = lambda hlist,env: mtest(hlist[0], env)
                        test_to_add.name='matching(pdu)'
                    else:
                        test_to_add = mtest
                elif _element in RuleSetFactory.__registry:
                    test_to_add = named_function(partial(run_registered_function, RuleSetFactory.__registry[_element]),
                                                 name = element)
                else:
                    raise RuntimeError("Syntax Error: unrecognized condition {0!r}".format(element))

                if negated:
                    test_to_add = NegatedTest(test_to_add)

            result.append(test_to_add)

        return result

    def parse(self, sourcename, scope={}, *args, **kw):
        """Returns a RuleSet or a group of them, depending on the input.

           This function works on the new style bottom-up files"""

        if sourcename in self._cache:
            return self._cache[sourcename]

        source = self._cls.get_raw(sourcename)

        data = yaml.load(source)
        if not data:
            return self._cls.build_empty(sourcename, *args, **kw)

        keywordDescr = {}
        rangeRestrictions = {}
        conditions = AndTest(name='conditions')
        postConditions = AndTest(name='postConditions')
        features = []

        #for entry in data:
        #    if entry not in reserved_entries:
        #        raise RuntimeError("Syntax Error: {0!r} (on {1})".format(entry, sourcename))

        # conditions keeps lists of tests that will be performed on the
        # header to figure out if this ruleset applies or not.
        conditions = self.__parse_tests(sourcename, data.get('conditions', []), name='conditions')

        features = list(iter_list(data.get('provides')))

        # This one takes a list of definition files.
        if 'one of' in data:
            for entry in data:
                if entry not in {'conditions', 'provides', 'one of'}:
                    raise RuntimeError("{1}: {0!r} not compatible with 'one of'".format(entry, sourcename))

            alt = AlternateRuleSets()
            alt.set_attributes(cond=conditions, feat=features)
            for entry in data['one of']:
                alt.add(self.parse(entry, scope=scope, *args, **kw))

            res = alt
        else:
            scope = scope.copy()
            for entry in iter_list(data.get('import')):
                scope[entry] = self.parse(entry, *args, **kw)

            local_scope = set([x for x in data if x not in reserved_global_identifiers])
            # Sort out illegal semantics early on
            if local_scope:
                for key in ('keywords', 'tests', 'merge', 'merge-maybe'):
                    if key in data:
                        raise RuntimeError("{0}: Found a global-level '{1}' along with validation units".format(sourcename, key))

            units = {}
            for unit in local_scope:
                new_unit = self.parse_validation_unit(sourcename, unit, data[unit], scope, *args, **kw)
                scope[unit] = units[unit] = new_unit

            merges = [get_from_scope(sourcename, scope, m) for m in iter_list(data.get('merge', []))]
            maybe_merges = [get_from_scope(sourcename, scope, m) for m in iter_list(data.get('maybe-merge', []))]

            # postConditions is similar to conditions in that it holds tests to be
            # performed over the header contents, and accept the same syntax, but they're run
            # once we know that the ruleset is actually applies to the current HDU and that
            # the mandatory keywords are all there. It's used mostly for complex logic
            postConditions = self.__parse_tests(sourcename, data.get('tests', []), name='postConditions')

            rangeRestrictions = dict(iter_pairs(data.get('range limits'), Range.from_string))

            try:
                keywordDescr = dict(iter_keywords(data.get('keywords')))
            except ValueError as e:
                s = str(e)
                raise ValueError('{0}: {1}'.format(sourcename, s))

            # Prepare the validation section, if there's any
            valdct = dict(iter_pairs(data.get('validation', []) + data.get('validation(final)', [])))
            invalid_valdct = [x for x in valdct if x not in {'primary-hdu', 'extension'}]
            if invalid_valdct:
                text = ('entry {0!r}'.format(invalid_valdct[0])
                            if len(invalid_valdct) == 1
                            else 'entries ' + ', '.join('{0!r}'.format(x) for x in invalid_valdct))
                raise RuntimeError("{0}: At 'validation', illegal {1}".format(sourcename, text))

            validation = defaultdict(AlternateRuleSets)
            for key, value in valdct.items():
                for name in iter_list(value):
                    validation[key].add(get_from_scope(sourcename, scope, name))

            rset = self._cls(sourcename, *args, **kw)
            rset.set_attributes(
                cond = conditions,
                feat = features,
                keyw = keywordDescr,
                rngr = rangeRestrictions,
                post = postConditions,
                vald = validation,
                unit = units,
                merg = merges,
                mmer = maybe_merges,
                final = 'validation(final)' in data
                )
            res = rset

        self._cache[sourcename] = res
        return res

    def parse_validation_unit(self, sourcename, unitname, data, scope, *args, **kw):
        fullname = '.'.join([sourcename, unitname])
        if isinstance(data, dict):
            data = [data]
        invalid_kw = [x for x in data if x.keys()[0] not in reserved_unit_identifiers]
        if invalid_kw:
            text = ('entry {0!r}'.format(invalid_kw[0])
                        if len(invalid_kw) == 1
                        else 'entries ' + ', '.join('{0!r}'.format(x) for x in invalid_kw))
            raise RuntimeError("{0}: illegal {1}".format(fullname, text))

        keywordDescr = {}
        conditions = AndTest()
        postConditions = AndTest()
        merges = []
        maybe_merges = []
        features = []
        for dct in data:
            # conditions keeps lists of tests that will be performed on the
            # header to figure out if this ruleset applies or not.
            conditions.extend(self.__parse_tests(fullname, dct.get('conditions', [])))
            features.extend(list(iter_list(dct.get('provides'))))

            merges.extend([get_from_scope(fullname, scope, m) for m in iter_list(dct.get('merge', []))])
            maybe_merges.extend([get_from_scope(fullname, scope, m) for m in iter_list(dct.get('maybe-merge', []))])

            try:
                keywordDescr.update(dict(iter_keywords(dct.get('keywords'))))
            except ValueError as e:
                s = str(e)
                raise ValueError('{0}: {1}'.format(fullname, s))

            # postConditions is similar to conditions in that it holds tests to be
            # performed over the header contents, and accept the same syntax, but they're run
            # once we know that the ruleset is actually applies to the current HDU and that
            # the mandatory keywords are all there. It's used mostly for complex logic
            postConditions.extend(self.__parse_tests(sourcename, dct.get('tests', [])))

        ret = self._cls(fullname, *args, **kw)
        ret.set_attributes(
            cond = conditions,
            feat = features,
            keyw = keywordDescr,
            post = postConditions,
            merg = merges,
            mmer = maybe_merges,
            )
        return ret


class RuleSet(list):
    """RuleSet is a representation of one of the rule files. It contains
       restrictions for some keywords (mandatory or not, type, format...)
       and acts also as a container for further rulesets that are activated
       depending on the contents of a FITS header"""

    @classmethod
    def get_raw(cls, filename):
        return open(get_full_path(filename)).read()

    def __init__(self, filename):
        super(RuleSet, self).__init__()

        self.fn = filename
        self.keywordDescr = {}
        self.rangeRestrictions = {}
        self.conditions = AndTest()
        self.postConditions = AndTest()
        self.features = []
        self.merges = []
        self.maybe_merges = []
        self.units = {}
        self.validation = {}
        self.final = False

    def set_attributes(self, **kw):
        self.keywordDescr = kw.get('keyw', self.keywordDescr)
        self.rangeRestrictions = kw.get('rngr', self.rangeRestrictions)
        self.conditions = kw.get('cond', self.conditions)
        self.postConditions = kw.get('post', self.postConditions)
        self.features = kw.get('feat', self.features)
        self.validation = kw.get('vald', self.validation)
        self.units = kw.get('unit', self.units)
        self.merges = kw.get('merg', self.merges)
        self.maybe_merges = kw.get('mmer', self.maybe_merges)
        self.final = kw.get('final', self.final)

    def test(self, hlist, env):
        prev_final = env.final
        try:
            messages = []
            env.final |= self.final
            if self.validation:
                # We're working with a file descriptor
                results = []
                # There is probably always going to be primary-hdu, but...
                if 'primary-hdu' in self.validation:
                    res, mess = self.validation['primary-hdu'].validate(hlist[0], env)
                    results.append(res)
                    messages.append(mess)
                if 'extension' in self.validation:
                    for n, ext in enumerate(hlist[1:], 1):
                        res, mess = self.validation['extension'].validate(ext, env)
                        results.append(res)
                        messages.append(mess)

                return all(results), messages
            else:
                env.overrides.push(self.keywordDescr.keys())
                # First, try to pull in all mergeable things
                try:
                    for mergeable in self.merges:
                        res, mess = mergeable.validate(hlist, env)
                        if not res:
                            return res, mess
                    for mergeable in self.maybe_merges:
                        res, mess = mergeable.validate(hlist, env)
                        # maybe-merge means that the test may not be applicable
                        # Return now only if there are error messages...
                        if not res and mess:
                            return res, mess
                finally:
                    env.overrides.pop()

                # We're working with a unit descriptor
                for kw, descr in self.keywordDescr.items():
                    if kw in env.overrides or descr.ignore(hlist, env):
                        continue

                    try:
                        if not descr.test(hlist[kw]):
                            messages.append('Invalid {0}({1})'.format(kw, hlist[kw]))
                    except KeyError:
                        if descr.mandatory:
                            messages.append('Missing {0}'.format(kw))
                for kw, range in self.rangeRestrictions.items():
                    try:
                        if hlist[kw] not in range:
                            messages.append('Invalid {0}'.format(kw))
                    except KeyError:
                        # A missing keyword when checking for ranges is not relevant
                        pass

                if not messages:
                    for test in self.postConditions:
                        if not test(hlist, env):
                            messages.append('Failed {0}'.format(test.name))

                return len(messages) == 0, messages
        finally:
            if self.final:
                env.keeptesting = False
            env.final = prev_final

    def applies_to(self, hlist, env):
        return env.keeptesting and self.conditions(hlist, env)

    def validate(self, hlist, env):
        if not self.applies_to(hlist, env) and not env.final:
            return False, []

        return self.test(hlist, env)

    def __repr__(self):
        return "<{0} '{1}' [{2}]>".format(self.__class__.__name__,
                                          self.fn,
                                          ', '.join(self.units))

    def __hash__(self):
        return hash(self.__class__.__name__ + '_' + self.fn)

class AlternateRuleSets(object):
    """This class is an interface to multiple RuleSets. It chooses among a number of
       alternate rulesets and offers the same behaviour as the first one that matches the
       current environment and headers"""
    def __init__(self):
        self.alts = []
        self.conditions = AndTest()
        self._features = []
        self.winner = None

    @property
    def fn(self):
        return " | ".join(x.fn for x in self.alts)

    @property
    def features(self):
        return list(self._features) + list(self.winner.features)

    def __iter__(self):
        for k in self.winner:
            yield k

    def add(self, alt):
        self.alts.append(alt)

    def set_attributes(self, cond=None, feat=None):
        if cond:
            self.conditions = cond
        if feat:
            self._features = feat

    def validate(self, hlist, env):
        if not self.applies_to(hlist, env):
            return False, []
        return self.test(hlist, env)

    def applies_to(self, hlist, env):
        return self.conditions.test(hlist, env) and any(x.applies_to(hlist, env) for x in self.alts)

    def test(self, hlist, env):
        collect = []
        for alt in self.alts:
            valid, messages = alt.validate(hlist, env)
            if valid:
                self.winner = alt
                log("   - Choosing {0}".format(alt.fn))
                return True, []
            else:
                for m in messages:
                    log("   - {0}".format(m))
            collect.extend(messages)

        return False, collect

class RuleCollection(object):
    """Used to "stack up" RuleSet objects as they're activated by headers.
       It offers an interface to check the validity of a header."""

    def __init__(self, ruleSetClass = RuleSet):
        self.entryPoint = None
        self._ruleSetClass = ruleSetClass

    def initialize(self, mainFileName):
        self.entryPoint = RuleSetFactory(self._ruleSetClass).parse(mainFileName)

    @property
    def initialized(self):
        return self.entryPoint is not None

    def test(self, headerlist, env):
        ret, mess = self.entryPoint.validate(headerlist, env)
        if not ret and not mess:
            return False, [[NOT_FOUND_MESSAGE]]
        return ret, mess

Result = namedtuple('Result', ['passes', 'code', 'message'])

class Evaluator(object):
    def __init__(self, ruleSetClass=RuleSet):
        self.rq = RuleCollection(ruleSetClass)

    def init(self, root_file='root'):
        self.rq.initialize(root_file)

    def _set_initial_features(self, fits, tags):
        return set()

    def validate_file(self, fits, tags):
        """Evaluates the validity of a FITS file and returns a tuple (valid, messages, environment),
           where `valid` is a boolean, `messages` is a list of error messages, if there were any, and
           `environment` is the Environment object with the features collected during the evaluation"""
        if not self.rq.initialized:
            self.init()

        fits.verify('exception')
        env = Environment()
        env.features = self._set_initial_features(fits, tags)
        env.final = False
        env.keeptesting = True
        env.overrides = OverrideStack()

        return self.rq.test([hdu.header for hdu in fits], env) + (env,)

    def evaluate(self, fits, tags=set()):
        try:
            valid, msg, env = self.validate_file(fits, tags)
            # Skim non-strings from msg
            msg = [[x for x in m if not isinstance(x, RuleSet)] for m in msg]
            if valid:
                return Result(True, 'CORRECT', "This looks like a valid file")
            else:
                # First, focus on the PHDU messages
                if len(msg[0]) > 0:
                    # Errors in the PHDU - we won't consider the rest
                    rmsg = ['--- HDU 0 ---']
                    mset = set(msg[0])
                    if set([NOT_FOUND_MESSAGE]) == mset:
                        rmsg.extend([NOT_FOUND_MESSAGE])
                    else:
                        rmsg.extend([m for m in msg[0] if m != NOT_FOUND_MESSAGE])
                    if len(msg) > 1:
                        rmsg.extend(['-------------', 'Other HDUs not considered to avoid bogus error messages'])
                else:
                    rmsg = []
                    for hdu, hdumsg in enumerate(msg[1:], 1):
                        rmsg.append('--- HDU {0} ---'.format(hdu))
                        mset = set(hdumsg)
                        if set([NOT_FOUND_MESSAGE]) == mset:
                            rmsg.extend([NOT_FOUND_MESSAGE])
                        else:
                            rmsg.extend([m for m in hdumsg if m != NOT_FOUND_MESSAGE])
                return Result(False, 'NOPASS', '\n'.join(rmsg))
        except NoDateError:
            # NoDateError was used to simplify grouping certain common errors
            # when evaluating old data. It's a subset of the invalid headers,
            # and thus 'NOPASS'
            return Result(False, 'NOPASS', "No observing date could be recovered from the image headers")
        except EngineeringImage:
            return Result(True, 'ENG', "This looks like an engineering image. No further checks")
        except pf.VerifyError as e:
            return Result(False, 'EXCEPTION', str(e))

    def __call__(self, filename):
        return self.evaluate(filename)
