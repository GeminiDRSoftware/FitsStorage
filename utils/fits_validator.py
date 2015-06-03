#!/usr/bin/env python

from __future__ import print_function

import os
import re
import sys
from collections import namedtuple
from datetime import datetime
from time import strptime
from types import FunctionType

from fits_storage_config import validation_def_path
import gemini_metadata_utils as gmu

import yaml
import pyfits as pf

# Exceptions

class EngineeringImage(Exception):
    pass

# Constants

# This is used to determine things like if we test for IAA or OBSCLASS
# This is an initial estimate using empirical data. The value must
# be corrected at a later point.
OLDIMAGE = datetime(2004, 10, 20)
OBSCLASS_VALUES = {'dayCal',  'partnerCal',  'acqCal',  'acq',  'science',  'progCal'}
DEBUG = False

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
    lambda x: datetime(*strptime(x, "%Y-%m-%d %H:%M:%S")[:6]),
    lambda x: datetime(*strptime(x, "%Y-%m-%d (%H:%M:%S)")[:6]),
    lambda x: datetime(*strptime(x, "%Y-%m-%dT%H:%M:%S")[:6]),
    )

###################################################################################

def coerceValue(val):
    for fn in typeCoercion:
        try:
            return fn(val)
        except ValueError:
            pass

    raise ValueError('{0} not a known FITS value'.format(val))

def log(text):
    if DEBUG:
        print(text)

Function = namedtuple('Function', ['name', 'code', 'exceptionIfTrue', 'exceptionIfFalse'])

class Range(object):
    def __init__(self, low, high, type):
        self.low, self.high, self.type = low, high, type

    @staticmethod
    def from_string(string, forceType = None):
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
                        elif args['type'] != tp:
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
        "Implements the membership test operator, allowing <x in range>"
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

        return ((type(x) == self.type) and
                ((self.low is None) or (self.low <= x)) and
                ((self.high is None) or (self.high >= x)))

    def __str__(self):
        return "{0} .. {1}".format((str(self.low) if self.low else '*'),
                                   (str(self.high) if self.high else '*'))

EmptyRange = Range.from_type(None)

def not_implemented(fn):
    def wrapper(self, *args, **kw):
        raise NotImplementedError("{0}.{1}".format(self.__class__.__name__,
                                                                    fn.func_name))
    return wrapper

def get_full_path(filename):
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

class KeywordDescriptor(object):
    def __init__(self, info):
        self.range = EmptyRange
        self.fn = lambda x: x

        for restriction in info:
            if isinstance(restriction, (str, unicode)):
                if restriction in fitsTypes:
                    self.range = Range.from_type(fitsTypes[restriction])
                elif restriction == 'upper':
                    self.fn = str.upper
                elif restriction == 'lower':
                    self.fn = str.lower
                else:
                    raise ValueError("Unknown descriptor {0}".format(restriction))
            if isinstance(restriction, dict):
                kw, value = restriction.items()[0]
                if kw in fitsTypes:
                    if kw == 'char':
                        self.range = set(iter_list(value))
                    else:
                        self.range = Range.from_string(value, forceType = fitsTypes[kw])
                else:
                    raise ValueError("Unknown descriptor {0}".format(restriction))

    def test(self, value):
        return self.fn(value) in self.range

def test_inclusion(v1, v2, *args, **kw):
    if isinstance(v2, (str, unicode)):
        return v1 == v2
    return v1 in v2

def run_function(func, header, env):
    res = func.code(header, env)
    if res and func.exceptionIfTrue is not None:
        raise func.exceptionIfTrue()
    elif not res and func.exceptionIfFalse is not None:
        raise func.exceptionIfFalse()

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

# Extra functions to help RuleSet conditions
def hdu_in(hdus, h, env):
    return env.hduNum in hdus
def in_environment(val, h, env, negate = False):
    r = val in env.features

    if negate:
        return not r

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

def ruleFactory(text):
    # We don't want to write complicated parsers, but if we would want to generalize
    # the syntax, we could use the following EBNF:
    #
    #   list = ( group "," )* group
    #  group = ( word "|")* word | "(" list ")"
    if "|" in text:
        return AlternateRuleSets([RuleSet(x.strip()) for x in text.split('|')])

    return RuleSet(text)

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

    def __init__(self, filename):
        self.fn = filename

        self.__initalize(filename)

    def __initalize(self, filename):
        with open(get_full_path(filename)) as source:
            data = yaml.load(source)
            for entry in data:
                if entry not in valid_entries:
                    raise RuntimeError("Syntax Error: {0!r} (on {1})".format(entry, self.fn))
            self.features = list(iter_list(data.get('provides')))
            try:
                self.keywordDescr = dict((key, KeywordDescriptor(value))
                                            for (key, value)
                                             in iter_pairs(data.get('keywords')))
            except ValueError as e:
                s = str(e)
                raise ValueError('{0}: {1}'.format(self.fn, s))
            self.rangeRestrictions = dict(iter_pairs(data.get('range limits'), Range.from_string))
            # conditions keeps lists of tests that will be performed on the
            # header to figure out if this ruleset applies or not. A hit on
            # any of the "False" ones will exclude the ruleset. If no excluding
            # conditions are met, then the "True" ones are used to figure if it
            # conditions for inclusion are met
            self.conditions = self.__parse_tests(data.get('conditions', []))
            # postConditions is similar to conditions in that it holds tests to be
            # performed over the header contents, and accept the same syntax, but they're run
            # once we know that the ruleset is actually applies to the current HDU and that
            # the mandatory keywords are all there. It's used mostly for complex logic and
            # ALL of them have to pass
            r = self.__parse_tests(data.get('tests', []))
            self.postConditions = r[True] + r[False]
            for inc in iter_list(data.get('include files')):
                self.append(ruleFactory(inc))

    def __parse_tests(self, data):
        result = { True: [], False: [] }

        for entry in data:
            if isinstance(entry, (str, unicode)):
                element, content = entry, []
            elif isinstance(entry, dict):
                element, content = entry.items()[0]
            else:
                raise RuntimeError("Syntax Error: Invalid entry, {0!r} (on {1})".format(entry, self.fn))

            if element == 'on hdus':
                result[False].append(callback_factory(hdu_in, set(iter_list(content)), name = 'on hdus'))
            if element.startswith('is '):
                if element.startswith('is not '):
                    ng = False
                    elem = ' '.join(element.split()[2:])
                else:
                    ng = True
                    elem = ' '.join(element.split()[1:])
                result[False].append(callback_factory(in_environment, elem, negate = ng, name = element))
            else:
                if element.startswith('not '):
                    inclusive = False
                    _element = ' '.join(element.split()[1:])
                else:
                    inclusive = True
                    _element = element

                if _element == 'exists':
                    for kw in iter_list(content):
                        result[inclusive].append(callback_factory(kw, name = element))
                elif _element == 'matching':
                    for kw, val in iter_pairs(content):
                        result[inclusive].append(callback_factory(kw, val, name = element))
                elif _element in RuleSet.__registry:
                    result[inclusive].append(callback_factory(RuleSet.__registry[_element], name = element))
                else:
                    print(element)
                    raise RuntimeError("Syntax Error: unrecognized condition {0!r}".format(element))

        return result

    def test(self, header, env):
        messages = []
        for kw, descr in self.keywordDescr.items():
            try:
                if not descr.test(header[kw]):
                    messages.append('Invalid {0}({1})'.format(kw, header[kw]))
            except KeyError:
                messages.append('Missing {0}'.format(kw))
        for kw, range in self.rangeRestrictions.items():
            try:
                if header[kw] not in range:
                    messages.append('Invalid {0}'.format(kw))
            except KeyError:
                # A missing keyword when checking for ranges is not relevant
                pass

        for test in self.postConditions:
            if not test(header, env):
                messages.append('Failed {0}'.format(test.name))
        return messages

    def applies_to(self, header, env):
        incTests, excTests = self.conditions[True], self.conditions[False]

        include = (any(test(header, env) for test in incTests)
                   if incTests
                   else True)
        exclude = any(test(header, env) for test in excTests)

        return not exclude and include

    def __repr__(self):
        return "<RuleSet '{0}' [{1}]>".format(self.fn, ', '.join(x.fn for x in self), ', '.join(self.keywordDescr))

class AlternateRuleSets(object):
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

        for k in self.alts:
            messages = k.test(header, env)
            if not messages:
                self.winner = k
                return []
            collect.extend(messages)

        return collect

class RuleStack(object):
    """Used to "stack up" RuleSet objects as they're activated by headers.
       It offers an interface to check the validity of a header."""

    def __init__(self):
        self.entryPoint = None

    def initialize(self, mainFileName):
        self.entryPoint = RuleSet(mainFileName)

    def test(self, header, env):
        stack = [self.entryPoint]
        passed = []

        while stack:
            ruleSet = stack.pop()
            mess = ruleSet.test(header, env)
            if len(mess) > 0:
                return (False, mess)

            passed.append(ruleSet)
            env.features.update(ruleSet.features)

            for candidate in ruleSet:
                if candidate.applies_to(header, env):
                    log("  - Expanding {0}".format(candidate.fn))
                    stack.append(candidate)

        try:
            env.features.remove('valid')
        except KeyError:
            mess.append("Could not find a validating set of rules")
            return (False, mess)

        return (True, passed)

# Here, custom functions

@RuleSet.register_function("engineering", excIfTrue = EngineeringImage)
def engineering_image(header, env):
    "Naive engineering image detection"
    prgid = header.get('GEMPRGID', '')
    return prgid.startswith('GN-ENG') or prgid.startswith('GS-ENG')

@RuleSet.register_function("calibration")
def calibration_image(header, env):
    "Naive calib image detection"
    prgid = header.get('GEMPRGID', '')
    fromId = prgid.startswith('GN-CAL') or prgid.startswith('GS-CAL')
    return fromId or (header.get('OBSCLASS') == 'dayCal')

@RuleSet.register_function("wcs-after-pdu")
def wcs_in_extensions(header, env):
    if header.get('FRAME').upper() in ('AZEL_TOPO', 'NO VALUE'):
        env.features.add('no-wcs-test')

    return True

@RuleSet.register_function("should-test-wcs")
def wcs_or_not(header, env):
    feat = env.features
    return (    ('no-wcs-test' not in feat)
            and (   ('wcs-in-pdu' in feat and 'XTENSION' not in header)
                 or ('wcs-in-pdu' not in feat and header.get('XTENSION') == 'IMAGE')))

rawxx_pattern = re.compile(r'Any|\d{2}-percentile')

@RuleSet.register_function("valid-rawXX")
def check_rawXX_contents(header, env):
    return all((header[x].upper() == 'UNKNOWN' or rawxx_pattern.match(header[x]) is not None)
                for x in ('RAWBG', 'RAWCC', 'RAWIQ', 'RAWWV'))

@RuleSet.register_function("valid-observation-info", excIfFalse = EngineeringImage)
def check_observation_related_fields(header, env):
    prg = gmu.GeminiProgram(header['GEMPRGID'])
    obs = gmu.GeminiObservation(header['OBSID'])
    dl  = gmu.GeminiDataLabel(header['DATALAB'])

    return (prg.valid and obs.obsnum != '' and dl.dlnum != ''
                      and obs.obsnum == dl.obsnum
                      and prg.program_id == obs.program.program_id == dl.projectid)

@RuleSet.register_function('valid-iaa-and-obsclass')
def check_iaa_and_obsclass_in_recent_images(header, env):
    if coerceValue(header['DATE-OBS']) < OLDIMAGE:
        return True

    try:
        return (header['OBSCLASS'] in OBSCLASS_VALUES) and ('IAA' in header)
    except KeyError:
        return False

if __name__ == '__main__':
    DEBUG = True
    try:
        env = Environment()
        env.features = set()
        rs = RuleStack()
        rs.initialize('fits')
        fits = pf.open(sys.argv[1])
        fits.verify('fix+exception')
        err = 0
        for n, hdu in enumerate(fits):
            env.hduNum = n
            log("* Testing HDU {0}".format(n))
            res, args = rs.test(hdu.header, env)
            if not res:
                err += 1
                for message in args:
                    log("   - {0}".format(message))
            elif not args:
                err += 1
                log("  No key ruleset found for this HDU")

    except EngineeringImage:
        log("Its an engineering image")
        err = 1
    except RuntimeError as e:
        log(str(e))
        err = 1
    if err > 0:
        sys.exit(-1)
    sys.exit(0)
