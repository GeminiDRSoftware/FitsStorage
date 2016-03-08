import re
from .adapter import get_context, Return
from urlparse import parse_qs

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
    def __init__(self, string, action=None, methods=None, redirect_to=None,
                 defaults=None, strict=False, collect_qs_args=None):
        # TODO: Assert that there's a value for either action or redirect_to
        self.string = string
        self.action = action
        self.strict = strict
        self.redirect_to = redirect_to
        if methods is not None:
            self.methods = set(methods)
            if 'GET' in methods:
                self.methods.add('HEAD')
        else:
            self.methods = None
        self.defaults = defaults or {}
        self.qs_mapping = collect_qs_args or {}

        self._regex = None
        self._variables  = {}
        self._converters = {}
        self.this        = None

    def compile(self, map_):
        reg_parts = []
        # "Variable number". Used to give each pattern variable a unique name. Useful
        # mainly when a converter translates its results to a tuple of values, instead
        # of a single one
        varn = 1
        first_static = True
        add_variable_slash = not (self.strict or self.string.endswith('/'))
        for (converter, arguments, variable) in parse_rule(self.string):
            if converter is None:
                # Static part of the URL
                reg_parts.append(re.escape(variable))
                if first_static:
                    spl = variable.split('/')
                    if len(spl) == 1:
                        self.this = '/'
                    else:
                        self.this = spl[1] or '/'
                    first_static = False
            else:
                if arguments:
                    c_args, c_kwargs = parse_converter_args(arguments)
                else:
                    c_args, c_kwargs = (), {}
                convobj = map_.get_converter(variable, converter, c_args, c_kwargs)
                varseq = 'VAR{}'.format(varn)
                self._variables[varseq] = (variable if ',' not in variable else tuple(v.strip() for v in variable.split(',')))
                self._converters[varseq] = convobj
                reg_parts.append('(?P<{}>{})'.format(varseq, convobj.regex))
                varn = varn + 1
        regex = r'^{}{}$'.format(u''.join(reg_parts), '/?' if add_variable_slash else '')
        self._regex = re.compile(regex, re.UNICODE)

    def match(self, path):
        add_slash = not (self.strict or path.endswith('/'))
        res = self._regex.search(path if not add_slash else (path + '/'))
        if res:
            gd = res.groupdict()
            result = []
            added = set()
            for name, value in gd.iteritems():
                try:
                    var = self._variables[name]
                    val = self._converters[name].to_python(value)
                    if isinstance(var, tuple):
                        result.append(dict(zip(var, val)))
                        for k in var:
                            added.add(k)
                    else:
                        result.append({var: val})
                        added.add(var)
                except ValueError:
                    return

            if self.qs_mapping:
                qs_args = parse_qs(get_context().req.env.qs)
                for var, mapping in self.qs_mapping.iteritems():
                    if var in qs_args:
                        result.append({mapping: qs_args[var]})
                        added.add(mapping)

            for var, val in self.defaults.iteritems():
                if var not in added:
                    result.append({var: val})

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
        if converters is not None:
            for name, convclass in converters.iteritems():
                self.add_converter(name, convclass)
        for rule in rules or ():
            self.add(rule)
        self.forbidden = set()

    def add(self, rule):
        rule.compile(self)
        self._rules.append(rule)

    def add_converter(self, name, convclass):
        self.converters[name] = convclass

    def _split_url(self, url):
        return tuple(filter(len, url.split('/')))

    def add_forbidden(self, url):
        self.forbidden.add(self._split_url(url))

    def is_forbidden(self, url):
        return self._split_url(url) in self.forbidden

    def get_converter(self, variable, converter_name, args, kwargs):
        if converter_name not in self.converters:
            raise LookupError('the converter {!r} does not exist'.format(converter_name))
        return self.converters[converter_name](*args, **kwargs)

    def match(self, path_info, method=None):
        ctx = get_context()
        for rule in self._rules:
            m = rule.match(path_info)
            if m is not None:
                if method is not None and method not in rule.methods:
                    # Most probably we want to keep track of this, to raise an exception
                    # if there was a match but no compatible method
                    continue
                ctx.usagelog.this = rule.this
                if self.is_forbidden(rule.this):
                    ctx.resp.client_error(Return.HTTP_FORBIDDEN)
                elif rule.redirect_to is not None:
                    ctx.resp.redirect_to(rule.redirect_to)
                return rule.action, m
