#!/usr/bin/env python

from __future__ import print_function
__all__ = ['RuleSet', 'RuleStack', 'Environment', 'ValidationError', 'BadData',
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
from collections import namedtuple
from datetime import datetime
from time import strptime
from types import FunctionType

from ..fits_storage_config import validation_def_path

import yaml
import pyfits as pf

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
    'date': datetime
    }

rangePatterns = (
    re.compile(r'(\S+)\s+\.\.\s+(\S+)'),
    )

typeCoercion = (
    int,
    float,
    lambda x: datetime(*strptime(x, "%Y-%m-%d")[:6]),
    lambda x: datetime(*strptime(x, "%Y-%b-%d")[:6]),
    lambda x: datetime(*strptime(x, "%Y-%m-%d %H:%M:%S")[:6]),
    lambda x: datetime(*strptime(x, "%Y-%m-%d (%H:%M:%S)")[:6]),
    lambda x: datetime(*strptime(x, "%Y-%m-%dT%H:%M:%S")[:6]),
    )

###################################################################################

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

Function = namedtuple('Function', ['name', 'code', 'exceptionIfTrue', 'exceptionIfFalse'])

compatible_types = {
    float: (float, int),
    }

class CompositeRange(object):
    """This class is used to compose a number of range tests together"""
    def __init__(self):
        self.tests = []

    def __contains__(self, value):
        """Returns True if the passed value falls within any of the ranges
           contained by this composite"""
        if not self.tests:
            # No ranges defined
            return True

        return any((value in test) for test in self.tests)

    def append(self, test):
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
       passed by the user"""
    def __init__(self, test):
        self.test = test

    def __contains__(self, x):
        return self.test(x)

class TransformedStringRangeTest(object):
    def __init__(self, testfn, range):
        self.testfn = testfn
        self.range = range

    def __contains__(self, x):
        return isinstance(x, (str, unicode)) and self.testfn(x) in self.range

class Range(object):
    """Range testing class. Instances of this class check if a certain value is
       within the limits of a float (or integer) range"""
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

NotNull = ArbitraryRangeTest(lambda x: isinstance(x, (str, unicode)) and x != '')

def not_implemented(fn):
    "Decorator for not implemented tests"
    def wrapper(self, *args, **kw):
        raise NotImplementedError("{0}.{1}".format(self.__class__.__name__,
                                                                    fn.func_name))
    return wrapper

def get_full_path(filename):
    "Returns the full path to a rules file"
    return os.path.join(validation_def_path, filename + '.def')

def iter_list(lst):
    if isinstance(lst, (list, tuple)):
        for k in lst:
            yield k
    elif lst is not None:
        yield lst

def iter_pairs(lst, coercion = lambda x: x):
    for k in iter_list(lst):
        if isinstance(k, dict):
            for key, value in k.items():
                yield key, coercion(value)
        elif isinstance(k, (list, tuple)):
            yield (k[0], coercion(k[1:]))
        elif k is not None:
            yield (k, ())

def iter_keywords(lst):
    for (key, value) in iter_pairs(lst):
        if key.startswith('if '):
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

def getEnvDate(env):
    for feat in env.features:
        if feat.startswith('date:'):
            return coerceValue(feat[5:])

    raise NoDateError()

### Functions for skip test
# The functions in this section are meant to test if the conditions are
# met so that we can test for the existence and validity of a certain keyword.
#
# They must return False if this is NOT the case. Have this in mind when reading
# the code, because you may expect different results

def buildSinceFn(value):
    return lambda h, e: getEnvDate(e) < value

def buildEnvConditionFn(value):
    if value.startswith('not '):
        negated = True
        value = value[4:].strip()
    else:
        negated = False

    if negated:
        return lambda h, env: value in env.features
    return lambda h, env: value not in env.features

### End functions for skip test

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
        self.reqs = []
        self.transforms = []
        self.range = CompositeRange()
        self.optional = False
        self.fn = lambda x: x

        # Maybe we should warn when this is None...
        if info is not None:
            for restriction in info:
                self.addRestriction(restriction)

    def addRestriction(self, restriction):
        if isinstance(restriction, (str, unicode)):
            if restriction in fitsTypes:
                self.range.append(Range.from_type(fitsTypes[restriction]))
            elif restriction == 'optional':
                self.optional = True
            elif restriction.startswith('if '):
                self.reqs.append(buildEnvConditionFn(restriction[3:].strip()))
            else:
                raise ValueError("Unknown descriptor {0}".format(restriction))
        if isinstance(restriction, dict):
            kw, value = restriction.items()[0]
            if kw in fitsTypes:
                if kw == 'char':
                    if isinstance(value, str) and ' '.join(value.lower().split()) == 'not null':
                        self.range.append(NotNull)
                    else:
                        self.range.append(set(iter_list(value)))
                elif ' .. ' in value:
                    self.range.append(Range.from_string(value, forceType = fitsTypes[kw]))
                else:
                    self.range.append(set(iter_list(value)))
            elif kw == 'upper':
                self.addTransformedStringRangeTest(str.upper, value)
            elif kw == 'lower':
                self.addTransformedStringRangeTest(str.lower, value)
            elif kw == 'since':
                coerced = coerceValue(value)
                if not isinstance(coerced, datetime):
                    raise ValueError("Wrong value for 'since': {0}".format(value))
                self.reqs.append(buildSinceFn(coerced))
            elif kw == 'pattern':
                self.range.append(Pattern(value))
            else:
                raise ValueError("Unknown descriptor {0}".format(restriction))

    def addTransformedStringRangeTest(self, fn, value):
        self.range.append(TransformedStringRangeTest(fn, set(iter_list(value))))

    @property
    def mandatory(self):
        return not self.optional

    def skip(self, header, env):
        "Returns True if this descriptor applies to the given header in the current environment"
        if not self.reqs:
            return False

        return all(fn(header, env) for fn in self.reqs)

    def test(self, value):
        "Returns True if the passed value matches the descriptor"
        for fn in self.transforms:
            value = fn(value)

        return value in self.range

def test_inclusion(v1, v2, *args, **kw):
    if isinstance(v2, (str, unicode)):
        return v1 == v2
    return v1 in v2

def run_function(func, header, env):
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

valid_entries = {
    'conditions',
    'include files',
    'keywords',
    'provides',
    'range limits',
    'tests',
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

class NegatedTest(object):
    def __init__(self, test, pass_name=False):
        self._negated = test
        self.name = self._negated.name
        if not pass_name:
            self.name = 'not ' + self.name

    def test(self, header, env):
        return not self._negated(header, env)

    def __call__(self, header, env):
        return self.test(header, env)

class GroupTest(list):
    def __init__(self, name=None, conjunctive=True, *args, **kw):
        super(GroupTest, self).__init__(*args, **kw)
        self.name = name
        self._conj = conjunctive

    def test(self, header, env):
        if len(self) == 0:
            return True

        if self._conj:
            return all(test(header, env) for test in self)
        else:
            return any(test(header, env) for test in self)

    def __call__(self, header, env):
        return self.test(header, env)

# Extra functions to help RuleSet conditions
def hdu_in(hdus, h, env):
    return env.hduNum in hdus

def in_environment(val, h, env):
    r = val in env.features

    return r

def callback_factory(attr, value = None, name = 'Unknown test name', *args, **kw):
    if isinstance(attr, Function):
        l = lambda header, env: run_function(attr, header, env)
    elif isinstance(attr, FunctionType):
        if value is not None:
            l = lambda header, env: attr(value, header, env, *args, **kw)
        else:
            l = lambda header, env: attr(header, env, *args, **kw)
    elif isinstance(attr, (str, unicode)):
        if value is not None:
            l = lambda header, env: test_inclusion(header.get(attr), value, env)
        else:
            l = lambda header, env: attr in header
    else:
        raise RuntimeError("Don't know how to define a callback for [{0}, {1}]".format(attr, value))

    l.name = name
    return l

def ruleFactory(text, ruleSetClass):
    "Returns a RuleSet or a group of them, depending on the input"
    # We don't want to write complicated parsers, but if we would want to generalize
    # the syntax, we could use the following EBNF:
    #
    #   list = ( group "," )* group
    #  group = ( word "|")* word | "(" list ")"
    if "|" in text:
        return AlternateRuleSets([ruleSetClass(x.strip()) for x in text.split('|')])

    return ruleSetClass(text)

class RuleSet(list):
    """RuleSet is a representation of one of the rule files. It contains
       restrictions for some keywords (mandatory or not, type, format...)
       and acts also as a container for further rulesets that are activated
       depending on the contents of a FITS header

       This object will load new rulesets, in cascade"""

    __registry = {}

    @classmethod
    def register_function(cls, name, excIfTrue = None, excIfFalse = None):
        def reg(fn):
            cls.__registry[name] = Function(name = name, code = fn,
                                            exceptionIfTrue = excIfTrue,
                                            exceptionIfFalse = excIfFalse)

            return fn
        return reg

    def _open(self, filename):
        return open(get_full_path(filename))

    def __init__(self, filename):
        super(RuleSet, self).__init__()

        self.fn = filename
        self.keywordDescr = {}
        self.rangeRestrictions = {}
        self.conditions = GroupTest()
        self.postConditions = GroupTest()
        self.features = []

        self.__initalize(filename)

    def __initalize(self, filename):
        with self._open(filename) as source:
            data = yaml.load(source)
            if not data:
                return
            for entry in data:
                if entry not in valid_entries:
                    raise RuntimeError("Syntax Error: {0!r} (on {1})".format(entry, self.fn))
            self.features = list(iter_list(data.get('provides')))
            try:
                self.keywordDescr = dict(iter_keywords(data.get('keywords')))
            except ValueError as e:
                s = str(e)
                raise ValueError('{0}: {1}'.format(self.fn, s))
            self.rangeRestrictions = dict(iter_pairs(data.get('range limits'), Range.from_string))
            # conditions keeps lists of tests that will be performed on the
            # header to figure out if this ruleset applies or not.
            self.conditions = self.__parse_tests(data.get('conditions', []))
            # postConditions is similar to conditions in that it holds tests to be
            # performed over the header contents, and accept the same syntax, but they're run
            # once we know that the ruleset is actually applies to the current HDU and that
            # the mandatory keywords are all there. It's used mostly for complex logic
            self.postConditions = self.__parse_tests(data.get('tests', []))
            for inc in iter_list(data.get('include files')):
                self.append(ruleFactory(inc, self.__class__))

    def __parse_tests(self, data):
        result = GroupTest()

        for entry in data:
            if isinstance(entry, (str, unicode)):
                element, content = entry, []
            elif isinstance(entry, dict):
                element, content = entry.items()[0]
            else:
                raise RuntimeError("Syntax Error: Invalid entry, {0!r} (on {1})".format(entry, self.fn))

            negated = False
            if element.startswith('is '):
                if element.startswith('is not '):
                    negated = True
                    _element = element[6:].strip()
                else:
                    _element = element[2:].strip()
                test_to_add = callback_factory(in_environment, _element, name = element)

                if negated:
                    test_to_add = NegatedTest(test_to_add, pass_name = True)
            else:
                if element.startswith('not '):
                    negated = True
                    _element = element[3:].strip()
                else:
                    negated = False
                    _element = element

                if _element == 'on hdus':
                    test_to_add = callback_factory(hdu_in, set(iter_list(content)), name = _element)
                elif _element == 'exists':
                    test_to_add = GroupTest(name = _element)
                    for kw in iter_list(content):
                        test_to_add.append(callback_factory(kw, name = element))
                elif _element == 'matching':
                    test_to_add = GroupTest(name = _element)
                    for kw, val in iter_pairs(content):
                        test_to_add.append(callback_factory(kw, val, name = element))
                elif _element in RuleSet.__registry:
                    test_to_add = callback_factory(RuleSet.__registry[_element], name = element)
                else:
                    raise RuntimeError("Syntax Error: unrecognized condition {0!r}".format(element))

                if negated:
                    test_to_add = NegatedTest(test_to_add)

            result.append(test_to_add)

        return result

    def test(self, header, env):
        messages = []
        for kw, descr in self.keywordDescr.items():
            if descr.skip(header, env):
                continue

            try:
                if not descr.test(header[kw]):
                    messages.append('Invalid {0}({1})'.format(kw, header[kw]))
            except KeyError:
                if descr.mandatory:
                    messages.append('Missing {0}'.format(kw))
        for kw, range in self.rangeRestrictions.items():
            try:
                if header[kw] not in range:
                    messages.append('Invalid {0}'.format(kw))
            except KeyError:
                # A missing keyword when checking for ranges is not relevant
                pass

        if not messages:
            for test in self.postConditions:
                if not test(header, env):
                    messages.append('Failed {0}'.format(test.name))
        return messages

    def applies_to(self, header, env):
        return self.conditions.test(header, env)

    def __repr__(self):
        return "<{0} '{1}' [{2}]>".format(self.__class__.__name__, self.fn, ', '.join(x.fn for x in self), ', '.join(self.keywordDescr))

    def __hash__(self):
        return hash(self.__class__.__name__ + '_' + self.fn)

class AlternateRuleSets(object):
    """This class is an interface to multiple RuleSets. It chooses among a number of
       alternate rulesets and offers the same behaviour as the first one that matches the
       current environment and headers"""
    def __init__(self, alternatives):
        self.alts = alternatives
        self.winner = None

    @property
    def fn(self):
        return " | ".join(x.fn for x in self.alts)

    @property
    def features(self):
        return list(self.winner.features)

    def __iter__(self):
        for k in self.winner:
            yield k

    def applies_to(self, header, env):
        return any(x.applies_to(header, env) for x in self.alts)

    def test(self, header, env):
        collect = []

        for k in (x for x in self.alts if x.applies_to(header, env)):
            messages = k.test(header, env)
            if not messages:
                self.winner = k
                log("   - Choosing {0}".format(k.fn))
                return []
            else:
                for m in messages:
                    log("   - {0}".format(m))
            collect.extend(messages)

        return collect

class RuleStack(object):
    """Used to "stack up" RuleSet objects as they're activated by headers.
       It offers an interface to check the validity of a header."""

    def __init__(self, ruleSetClass = RuleSet):
        self.entryPoint = None
        self._ruleSetClass = ruleSetClass

    def initialize(self, mainFileName):
        self.entryPoint = self._ruleSetClass(mainFileName)

    @property
    def initialized(self):
        return self.entryPoint is not None

    def test(self, header, env):
        stack = [self.entryPoint]
        done = set()
        passed = []
        mess = []

        while stack:
            ruleSet = stack.pop(0)
            done.add(ruleSet)

            log("  - Expanding {0}".format(ruleSet.fn))
            tmess = ruleSet.test(header, env)

            if not tmess:
                passed.append(ruleSet)
            else:
                mess.extend(tmess)

            env.features.update(ruleSet.features)

            if 'failed' in env.features:
                return (True, [])

            for candidate in ruleSet:
                if candidate in done:
                    log("  - Not including {0}. I've seen it before".format(candidate.fn))
                    continue
                if candidate.applies_to(header, env):
                    stack.append(candidate)
                else:
                    log("  - Not applicable: {0}".format(candidate.fn))

        try:
            env.features.remove('valid')
        except KeyError:
            mess.append(NOT_FOUND_MESSAGE)

        if mess:
            return (False, mess)

        return (True, passed)

Result = namedtuple('Result', ['passes', 'code', 'message'])

class Evaluator(object):
    def __init__(self, ruleSetClass=RuleSet):
        self.rq = RuleStack(ruleSetClass)

    def init(self, root_file='fits'):
        self.rq.initialize(root_file)

    def _set_initial_features(self, fits, tags):
        return set()

    def valid_header(self, fits, tags):
        if not self.rq.initialized:
            self.init()

        fits.verify('exception')
        env = Environment()
        env.features = self._set_initial_features(fits, tags)
        res = []
        mess = []
        for n, hdu in enumerate(fits):
            env.numHdu = n
            t = self.rq.test(hdu.header, env)
            res.append(t[0])
            mess.append(t[1])

        return all(res), mess, env

    def evaluate(self, fits, tags=set()):
        try:
            valid, msg, env = self.valid_header(fits, tags)
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
