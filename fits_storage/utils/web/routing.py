import re

# This rule regular expressions is copied from werkzeug's, as we intend to make it
# syntax-compatible
_rule_re = re.compile(r'''
   (?P<static>[^<]*)                              # static rule data. Eg: /example/foo
   <
   (?:
     (?P<converter>[a-zA-Z_][a-zA-Z0-9_]*)        # Converter name
     (?:\((?P<args>.*?)\))?                       # converter arguments
     \:                                           # variable delimiter
   )?
   (?P<variable>[^>]+)           # variable name
   >
''', re.VERBOSE)

def parse_rule(rule):
    """
    Parse a rule and return it as a generator. Each iteration yields tuples
    in the form ``(converter, arguments, variable)``. If the converter is
    `None` it's a static url part, otherwise it's a dynamic one.

    Most of this function are copied straight from werkzeug's, as it does
    exactly what we want.
    """
    pos = 0
    end = len(rule)
    used_names = set()
    while pos < end:
        m = _rule_re.match(rule, pos)
        if m is None:
            break
        data = m.groupdict()
        if data['static']:
            yield None, None, data['static']
        variable = data['variable']
        converter = data['converter'] or 'default'
        if variable in used_names:
            raise ValueError('variable name {!r} used more than once.'.format(variable))
        used_names.add(variable)
        yield converter, data['args'] or None, variable
        pos = m.end()
    if pos < end:
        remaining = rule[pos:]
        if '>' in remaining or '<' in remaining:
            raise ValueError('malformed url rule: {!r}'.format(rule))
        yield None, None, remaining

def parse_converter_args(arguments):
    a = arguments.strip()
    if not a:
        return (), {}

    return tuple(x.strip() for x in a.split(',')), {}

class Rule(object):
    def __init__(self, string, action, methods=None):
        self.string = string
        self.action = action
        if methods is not None:
            self.methods = set(methods)
            if 'GET' in methods:
                self.methods.add('HEAD')
        else:
            self.methods = None

        self._regex = None
        self._converters = {}

    def compile(self, map_):
        reg_parts = []
        for (converter, arguments, variable) in parse_rule(self.string):
            if converter is None:
                # Static part of the URL
                reg_parts.append(re.escape(variable))
            else:
                if arguments:
                    c_args, c_kwargs = parse_converter_args(arguments)
                else:
                    c_args, c_kwargs = (), {}
                convobj = map_.get_converter(variable, converter, c_args, c_kwargs)
                self._converters[variable] = convobj
                reg_parts.append('(?P<{}>{})'.format(variable, convobj.regex))
        regex = r'^{}$'.format(u''.join(reg_parts))
        self._regex = re.compile(regex, re.UNICODE)

    def match(self, path):
        res = self._regex.search(path)
        if res:
            gd = res.groupdict()
            result = {}
            for name, value in gd.iteritems():
                try:
                    result[name] = self._converters[name].to_python(value)
                except ValueError:
                    return

            return result

class BaseConverter(object):
    regex = '[^/]+'
    @staticmethod
    def to_python(value):
        return value

class UnicodeConverter(BaseConverter):
    pass

class IntegerConverter(BaseConverter):
    regex = r'\d+'
    @staticmethod
    def to_python(value):
        return int(value)

DEFAULT_CONVERTERS = {
    'default': UnicodeConverter,
    'string':  UnicodeConverter,
    'int':     IntegerConverter
}

class Map(object):
    def __init__(self, rules=None, converters=None):
        self._rules = []
        self.converters = DEFAULT_CONVERTERS.copy()
        for rule in rules or ():
            self.add(rule)

    def add(self, rule):
        rule.compile(self)
        self._rules.append(rule)

    def get_converter(self, variable, converter_name, args, kwargs):
        if converter_name not in self.converters:
            raise LookupError('the converter {!r} does not exist'.format(converter_name))
        return self.converters[converter_name](*args, **kwargs)

    def match(self, path_info, method=None):
        for rule in self._rules:
            m = rule.match(path_info)
            if m is not None:
                if method is not None and method not in rule.methods:
                    # Most probably we want to keep track of this, to raise an exception
                    # if there was a match but no compatible method
                    continue
                return rule.action, m
