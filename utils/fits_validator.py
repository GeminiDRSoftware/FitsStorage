#!/usr/bin/env python

from __future__ import print_function

import os
import re
import sys
from collections import namedtuple
from datetime import datetime
from time import strptime
from types import FunctionType
from StringIO import StringIO

from fits_storage_config import validation_def_path
import gemini_metadata_utils as gmu

import yaml
import astropy.io.fits as pf
from astropy.time import Time

# Exceptions

class ValidationError(Exception):
    pass

class NotGeminiData(ValidationError):
    pass

class BadData(ValidationError):
    pass

class EngineeringImage(ValidationError):
    pass

class GeneralError(ValidationError):
    pass

class NoDateError(Exception):
    pass

# Constants
DEBUG = False
NOT_FOUND_MESSAGE = "Could not find a validating set of rules"
FACILITY_INSTRUME = {'bHROS', 'F2', 'GMOS-N', 'GMOS-S', 'GNIRS', 'GPI', 'GSAOI', 'NICI', 'NIFS', 'NIRI'}

# This is used to determine things like if we test for IAA or OBSCLASS
# This is an initial estimate using empirical data. The value must
# be corrected at a later point.
OLDIMAGE = datetime(2007, 06, 28)
OBSCLASS_VALUES = {'dayCal',  'partnerCal',  'acqCal',  'acq',  'science',  'progCal'}

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

compatible_types = {
    float: (float, int),
    }

class CompositeRange(object):
    def __init__(self):
        self.tests = []

    def __contains__(self, value):
        if not self.tests:
            # No ranges defined
            return True

        return any((value in test) for test in self.tests)

    def append(self, test):
        self.tests.append(test)

class Pattern(object):
    def __init__(self, pattern):
        self.cpat = re.compile(pattern)

    def __contains__(self, x):
        return self.cpat.match(x) is not None

    def __repr__(self):
        return '<pattern({0!r})>'.format(self.cpat.pattern)

class ArbitraryRangeTest(object):
    def __init__(self, test):
        self.test = test

    def __contains__(self, x):
        return self.test(x)

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

def getEnvDate(env):
    for feat in env.features:
        if feat.startswith('date:'):
            return coerceValue(feat[5:])

    raise NoDateError()

def buildSinceFn(value):
    return lambda h, e: getEnvDate(e) < value

class KeywordDescriptor(object):
    def __init__(self, info):
        self.reqs = []
        self.transforms = []
        self.range = CompositeRange()
        self.fn = lambda x: x

        # Maybe we should warn when this is None...
        if info is not None:
            for restriction in info:
                if isinstance(restriction, (str, unicode)):
                    if restriction in fitsTypes:
                        self.range.append(Range.from_type(fitsTypes[restriction]))
                    elif restriction == 'upper':
                        self.transforms.append(str.upper)
                    elif restriction == 'lower':
                        self.transforms.append(str.lower)
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
                    elif kw == 'since':
                        coerced = coerceValue(value)
                        if not isinstance(coerced, datetime):
                            raise ValueError("Wrong value for 'since': {0}".format(value))
                        self.reqs.append(buildSinceFn(coerced))
                    elif kw == 'pattern':
                        self.range.append(Pattern(value))
                    else:
                        raise ValueError("Unknown descriptor {0}".format(restriction))

    def skip(self, header, env):
        if not self.reqs:
            return False

        return all(fn(header, env) for fn in self.reqs)

    def test(self, value):
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

    @classmethod
    def _open(cls, filename):
        return open(get_full_path(filename))

    def __init__(self, filename):
        super(RuleSet, self).__init__()

        self.fn = filename
        self.keywordDescr = {}
        self.rangeRestrictions = {}
        self.conditions = []
        self.postConditions = []
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
                self.append(ruleFactory(inc, self.__class__))

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
                    raise RuntimeError("Syntax Error: unrecognized condition {0!r}".format(element))

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
        incTests, excTests = self.conditions[True], self.conditions[False]

        include = (any(test(header, env) for test in incTests)
                   if incTests
                   else True)
        exclude = any(test(header, env) for test in excTests)

        return not exclude and include

    def __repr__(self):
        return "<{0} '{1}' [{2}]>".format(self.__class__.__name__, self.fn, ', '.join(x.fn for x in self), ', '.join(self.keywordDescr))

class AlternateRuleSets(object):
    """This class is an interface to a number RuleSets. It chooses among a number of alternate
       rulesets and offers the same behaviour as the first one that matches the current
       environment and headers"""
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
        passed = []

        while stack:
            ruleSet = stack.pop()
            mess = ruleSet.test(header, env)
            if len(mess) > 0:
                return (False, mess)

            passed.append(ruleSet)
            env.features.update(ruleSet.features)

            if 'failed' in env.features:
                return (True, [])

            for candidate in ruleSet:
                if candidate.applies_to(header, env):
                    log("  - Expanding {0}".format(candidate.fn))
                    stack.append(candidate)

        try:
            env.features.remove('valid')
        except KeyError:
            mess.append(NOT_FOUND_MESSAGE)
            return (False, mess)

        return (True, passed)

# Here, custom functions
# TODO: We should be able to provide a reason for not passing a test

@RuleSet.register_function("is-gemini-data", excIfFalse = NotGeminiData)
def test_for_gemini_data(header, env):
    if 'XTENSION' in header:
        return True
    try:
        if gmu.gemini_instrument(header['INSTRUME'], gmos=True) is None:
            return False

        if header['INSTRUME'] in FACILITY_INSTRUME:
            env.features.add('facility')
        else:
            env.features.add('non-facility')

        return True

    except KeyError:
        return False

@RuleSet.register_function("engineering", excIfTrue = EngineeringImage)
def engineering_image(header, env):
    "Naive engineering image detection"
    if header.get('GEMENG') is True:
        return True

    if 'XTENSION' in header:
        return False
    try:
        prgid = str(header['GEMPRGID'])
        if prgid[:2] in ('GN', 'GS') and ('ENG' in prgid.upper()):
            return True

        if check_observation_related_fields(header, env) is not True:
            return True, "Does not look like a valid program ID"
    except KeyError:
        return True, "Missing GEMPRGID"

    return False
# Retest to figure out this one
#    except AttributeError as e:
#        return GeneralError("Testing GEMPRGID: " + str(e))

@RuleSet.register_function("calibration")
def calibration_image(header, env):
    "Naive calib image detection"
    prgid = header.get('GEMPRGID', '')
    try:
        fromId = prgid.startswith('GN-CAL') or prgid.startswith('GS-CAL')
    except AttributeError as e:
        return GeneralError("Testing GEMPRGID: " + str(e))
    return fromId or (header.get('OBSCLASS') == 'dayCal')

@RuleSet.register_function("wcs-after-pdu")
def wcs_in_extensions(header, env):
    try:
        if header.get('FRAME', '').upper() in ('AZEL_TOPO', 'NO VALUE'):
            env.features.add('no-wcs-test')
    except AttributeError:
        # In some cases FRAME is not a string...
        pass

    return True

@RuleSet.register_function("should-test-wcs")
def wcs_or_not(header, env):
    feat = env.features
    return (    ('facility' in feat or 'non-facility' in feat)
            and ('no-wcs-test' not in feat)
            and (   ('wcs-in-pdu' in feat and 'XTENSION' not in header)
                 or ('wcs-in-pdu' not in feat and header.get('XTENSION') == 'IMAGE')))

rawxx_pattern = re.compile(r'Any|\d{2}-percentile')

@RuleSet.register_function("valid-rawXX")
def check_rawXX_contents(header, env):
    return all((header[x].upper() == 'UNKNOWN' or rawxx_pattern.match(header[x]) is not None)
                for x in ('RAWBG', 'RAWCC', 'RAWIQ', 'RAWWV'))

@RuleSet.register_function("valid-observation-info", excIfFalse = EngineeringImage)
def check_observation_related_fields(header, env):
    prg = gmu.GeminiProgram(str(header['GEMPRGID']))
    obs = gmu.GeminiObservation(str(header['OBSID']))
    dl  = gmu.GeminiDataLabel(str(header['DATALAB']))

    valid = (prg.valid and obs.obsnum != '' and dl.dlnum != ''
                       and obs.obsnum == dl.obsnum
                       and prg.program_id == obs.program.program_id == dl.projectid)

    if not valid:
        return False, "Not a valid Observation ID"

    return True

@RuleSet.register_function('set-date')
def set_date(header, env):
    bogus = False
    for kw in ('DATE-OBS', 'DATE'):
        try:
            coerceValue(header[kw])
            env.features.add('date:' + header[kw])
            return True
        except KeyError:
            pass
        except ValueError:
            bogus = True

    if 'MJD_OBS' in header and header['MJD_OBS'] != 0.:
        d = Time(header['MJD_OBS'], format='mjd').datetime.strftime('%Y-%m-%d')
        env.features.add('date:' + d)
        return True

    if not bogus:
        return False, "Can't find DATE/DATE-OBS to set the date"
    else:
        return False, "DATE/DATE-OBS contains bogus info"

@RuleSet.register_function('failed-data', excIfTrue=BadData)
def check_for_bad_RAWGEMWA(header, env):
    return header.get('RAWGEMQA', '') == 'BAD'

Result = namedtuple('Result', ['passes', 'code', 'messages'])

class Evaluator(object):
    def __init__(self, ruleSetClass=RuleSet):
        self.rq = RuleStack(ruleSetClass)

    def init(self, root_file='fits'):
        self.rq.initialize(root_file)

    def valid_header(self, fits):
        if not self.rq.initialized:
            self.init()

        fits.verify('exception')
        env = Environment()
        env.features = set()
        res = []
        mess = []
        for n, hdu in enumerate(fits):
            env.numHdu = n
            t = self.rq.test(hdu.header, env)
            res.append(t[0])
            mess.extend(t[1])

        return all(res), mess

    def evaluate(self, fits):

        try:
            valid, msg = self.valid_header(fits)
            if valid:
                return Result(True, 'CORRECT', None)
            else:
                return Result(False, 'NOPASS', msg)
        except NoDateError:
            # NoDateError was used to simplify grouping certain common errors
            # when evaluating old data. It's a subset of the invalid headers,
            # and thus 'NOPASS'
            return Result(False, 'NOPASS', None)
        except NotGeminiData:
            return Result(False, 'NOTGEMINI', None)
        except BadData:
            return Result(False,  'BAD', None)
        except EngineeringImage:
            return Result(True, 'ENG', None)

    def __call__(self, filename):
        return self.evaluate(filename)

if __name__ == '__main__':
    argv = sys.argv[1:]
    verbose = False
    try:
        if argv[0] == '-v':
            verbose = True
            argv = argv[1:]
    except IndexError:
        pass

    try:
        fits = pf.open(argv[0])
    except IndexError:
        fits = pf.open(StringIO(sys.stdin.read()))
    fits.verify('fix+exception')

    if verbose:
        DEBUG = True
        try:
            env = Environment()
            env.features = set()
            rs = RuleStack()
            rs.initialize('fits')
            err = 0
            for n, hdu in enumerate(fits):
                env.hduNum = n
                log("* Testing HDU {0}".format(n))
                res, args = rs.test(hdu.header, env)
                if not res:
                    err += 1
                    for message in args:
                        log("   - {0}".format(message))
                elif 'failed' in env.features:
                    log("  Failed data")
                    break
                elif not args:
                    err += 1
                    log("  No key ruleset found for this HDU")

        except EngineeringImage as exc:
            s = str(exc)
            if not s:
                log("Its an engineering image")
            else:
                log("Its an engineering image: {0}".format(s))
            err = 0
        except NoDateError:
            log("This image has no recognizable date")
            err = 1
        except NotGeminiData:
            log("This doesn't look like Gemini data")
            err = 0
        except BadData:
            log("Failed data")
            err = 0
        except RuntimeError as e:
            log(str(e))
            err = 1
        if err > 0:
            sys.exit(-1)
    else:
        evaluate = Evaluator()
        result = evaluate(fits)
        if not result.passes:
            if result.messages is not None:
                mset = set(result.messages)
                if set([NOT_FOUND_MESSAGE]) == mset:
                    print(NOT_FOUND_MESSAGE)
                else:
                    for msg in result.messages:
                        if msg == NOT_FOUND_MESSAGE:
                            continue
                        print(" - {0}".format(msg))
    sys.exit(0)
