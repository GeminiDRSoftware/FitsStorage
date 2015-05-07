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

import yaml
import pyfits as pf

fitsTypes = {
    'char': str,
    'float': float,
    'int': int,
    'bool': bool,
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

class EngineeringImage(Exception):
    pass

DEBUG = False

def log(text):
    if DEBUG:
        print(text)

Function = namedtuple('Function', ['code', 'exception'])

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

        for restriction in info:
            if isinstance(restriction, (str, unicode)):
                if restriction in fitsTypes:
                    self.range = Range.from_type(fitsTypes[restriction])
                else:
                    raise ValueError("Unknown descriptor {0}".format(restriction))
            if isinstance(restriction, dict):
                kw, value = restriction.items()[0]
                if kw in fitsTypes:
                    self.range = Range.from_string(value, forceType = fitsTypes[kw])
                else:
                    raise ValueError("Unknown descriptor {0}".format(restriction))

    def test(self, value):
        return value in self.range

def test_inclusion(v1, v2, *args, **kw):
    if isinstance(v2, (str, unicode)):
        return v1 == v2
    return v1 in v2

def run_function(func, header, env):
    res = func.code(header, env)
    if res and func.exception is not None:
        raise func.exception()

    return res

valid_entries = {
    'conditions',
    'keywords',
    'include files',
    'range limits',
    'provides'
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

def callback_factory(attr, value = None, *args, **kw):
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

    return l

class RuleSet(list):
    """RuleSet is a representation of one of the rule files. It contains
       restrictions for some keywords (mandatory or not, type, format...)
       and acts also as a container for further rulesets that are activated
       depending on the contents of a FITS header

       This object will load new rulesets, in cascade"""

    __registry = {}

    @classmethod
    def register_function(cls, name, exception = None):
        def reg(fn):
            cls.__registry[name] = Function(code = fn, exception = exception)

            return fn
        return reg

    def __init__(self, filename):
        self.fn = filename

        self.keywordDescr = {}
        # inclusionTests keeps lists of tests that will be performed on the
        # header to figure out if this ruleset applies or not. A hit on
        # any of the "False" ones will exclude the ruleset. If no excluding
        # conditions are met, then the "True" ones are used to figure if it
        # conditions for inclusion are met
        self.inclusionTests = { True: [], False: [] }
        self.rangeRestrictions = {}
        self.features = []
        self.__initalize(filename)

    def __initalize(self, filename):
        with open(get_full_path(filename)) as source:
            data = yaml.load(source)
            for entry in data:
                if entry not in valid_entries:
                    raise RuntimeError("Syntax Error: {0!r} (on {1})".format(entry, self.fn))
            for feat in iter_list(data.get('provides')):
                self.features.append(feat)
            for inc in iter_list(data.get('include files')):
                self.append(RuleSet(inc))
            self.keywordDescr = dict((key, KeywordDescriptor(value))
                                        for (key, value)
                                         in iter_pairs(data.get('keywords')))
            self.rangeRestrictions = dict(iter_pairs(data.get('range limits'), Range.from_string))
            self.__initialize_conditions(data.get('conditions', []))

    def __initialize_conditions(self, data):

        for entry in data:
            if isinstance(entry, (str, unicode)):
                element, content = entry, []
            elif isinstance(entry, dict):
                element, content = entry.items()[0]
            else:
                raise RuntimeError("Syntax Error: Invalid entry, {0!r} (on {1})".format(entry, self.fn))

            if element == 'on hdus':
                self.inclusionTests[False].append(callback_factory(hdu_in, set(iter_list(content))))
            if element.startswith('is '):
                self.inclusionTests[False].append(callback_factory(in_environment, ' '.join(element.split()[1:]), negate = True))
            else:
                if element.startswith('not '):
                    inclusive = False
                    _element = ' '.join(element.split()[1:])
                else:
                    inclusive = True
                    _element = element

                if _element == 'exists':
                    for kw in iter_list(content):
                        self.inclusionTests[inclusive].append(callback_factory(kw))
                elif _element == 'matching':
                    for kw, val in iter_pairs(content):
                        self.inclusionTests[inclusive].append(callback_factory(kw, val))
                elif _element in RuleSet.__registry:
                    self.inclusionTests[inclusive].append(callback_factory(RuleSet.__registry[_element]))
                else:
                    raise RuntimeError("Syntax Error: unrecognized condition {0!r}".format(element))

    def test(self, header, env):
        messages = []
        for kw, descr in self.keywordDescr.items():
            try:
                if not descr.test(header[kw]):
                    messages.append('Invalid {0}'.format(kw))
            except KeyError:
                messages.append('Missing {0}'.format(kw))
        for kw, range in self.rangeRestrictions.items():
            try:
                if header[kw] not in range:
                    messages.append('Invalid {0}'.format(kw))
            except KeyError:
                # A missing keyword when checking for ranges is not relevant
                pass
        return messages

    def applies_to(self, header, env):
        incTests, excTests = self.inclusionTests[True], self.inclusionTests[False]

        include = (any(test(header, env) for test in incTests)
                   if incTests
                   else True)
        exclude = any(test(header, env) for test in excTests)

        return not exclude and include

    def __repr__(self):
        return "<RuleSet '{0}' [{1}] ({2})>".format(self.fn, ', '.join(x.fn for x in self), ', '.join(self.keywordDescr))

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

@RuleSet.register_function("engineering", EngineeringImage)
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
