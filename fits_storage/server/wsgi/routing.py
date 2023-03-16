import re
from fits_storage.server.wsgi.context import get_context
from fits_storage.server.wsgi.returnobj import Return

from urllib.parse import parse_qs

from fits_storage.web.selection import getselection

# This rule regular expressions is copied from werkzeug's, as we intend to make it
# syntax-compatible

# Why are we copying this code rather than just importing the module???

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
    """
    Describes a URL -> action mapping rule. There is only one required argument,
    ``string``. All other as optional, but either ``action`` or ``redirect_to``
    must be provided (and they are exclusive).

    ``string``
      This describes the URL to be matched. Must start with a slash (/), and may
      end with one -if not sure, add an extra slash at the end. Each component of
      the URL (the text in between two individual slashes) can be either an
      arbitrary piece of text, which will be treated as a static component, and
      matched exactly; or a variable. Variables are marked by enclosing them using
      angle brackets (``<var>``). A variable's name must be a valid Python identifier.

      Variable descriptors can be just a simple name, in which case the rule will
      just assign the captured text to that variable. It can be more complex, though,
      by specifying a converter (``<conv:var>``). Converters can accept arguments,
      which are enclosed by parentheses and separated by commas
      (``<conv(arg1,arg2):var>``). A converter may return more than one value in the
      form of a tuple, which we can assign to separate variable names
      (``<conv:var1,var2,var3>``).

    ``action``
      A callable that will produce the output associated with this URL. If the rule
      matches, this value will be returned, along with any variables that have been
      collected. The caller may use this information to invoke the callable passing
      the variables and their associated values as keyword arguments for the callable.

    ``redirect_to``
      A static URL. If it is defined, the caller may use it to trigger a redirection
      to this new address.

    ``methods``
      A sequence of acceptable methods. By default it is ``None``, meaning that any
      method will be ok. If ``'GET'`` is included in the list, ``'HEAD'`` will be
      included too (automatically).

    ``defaults``
      A dictionary of (variable, value) pairs that will be added to the ones to be
      passed to the action callable. Useful when we want to offer a number of
      different URLs that share the same callable, but provide only part of the
      required arguments.

    ``strict``
      Decides if the engine will match the URL exactly with regards to the
      slash at the end. Some applications distinguish two URLs that differ
      only on having a slash at the end, or not; while others ignore the
      difference.

      By default, ``strict`` is ``False``, meaning that we will match both
      cases.

    ``collect_qs_args``
      Query string arguments are the one passed as part of the URL, after
      a question mark, eg. ``?orderby=foo``. ``collect_qs_args`` is a dictionary
      where we map variables (the key) to a qs argument (the value) that may, or
      may not appear in the URL.

      If one of the qs arguments matched in the dictionary appears in the query,
      its associated value will be added to the variable list. If the action
      callable *expects* a value always, it would be wise to add another entry
      under ``defaults``, because the qs arguments are always treated as optional,
      and the URL will match even if they're not there.
    """
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
        regex = r'^{}{}$'.format(''.join(reg_parts), '/?' if add_variable_slash else '')
        self._regex = re.compile(regex, re.UNICODE)

    def match(self, path):
        """
        Tests if a certain ``path`` matches this rule. If it doesn't, the
        function will return ``None``. Otherwise, it returns a list with
        pairs variable:value, corresponding to the values captured from the
        URL path, qs arguments, and default values.
        """
        add_slash = not (self.strict or path.endswith('/'))
        res = self._regex.search(path if not add_slash else (path + '/'))
        if res:
            gd = res.groupdict()
            result = []
            added = set()
            for name, value in gd.items():
                try:
                    var = self._variables[name]
                    val = self._converters[name].to_python(value)
                    if isinstance(var, tuple):
                        result.append(dict(list(zip(var, val))))
                        for k in var:
                            added.add(k)
                    else:
                        result.append({var: val})
                        added.add(var)
                except ValueError:
                    return

            if self.qs_mapping:
                qs_args = parse_qs(get_context().req.env.qs)
                for var, mapping in self.qs_mapping.items():
                    if var in qs_args:
                        result.append({mapping: qs_args[var]})
                        added.add(mapping)

            for var, val in self.defaults.items():
                if var not in added:
                    result.append({var: val})

            return result


class BaseConverter(object):
    """
    Basic, minimal data converter. It captures a string of character
    that don't contain a slash on it.

    Contains a ``regex`` member that is used to capture the text string.

    Any derivative class should change the value of ``regex``, and/or
    reimplement the :py:meth:`to_python` method.
    """
    regex = '[^/]+'
    @staticmethod
    def to_python(value):
        """
        Takes the text representation of a captured value and (maybe)
        returns a processed version of it.

        The implementation for BaseConverter just returns the value
        as it was passed.
        """
        return value


class UnicodeConverter(BaseConverter):
    pass


class IntegerConverter(BaseConverter):
    """
    Takes
    """
    regex = r'\d+'
    @staticmethod
    def to_python(value):
        """
        Takes a value and returns the equivalent integer. It will raise
        :py:exc:`ValueError` if there is no possible conversion.
        """
        return int(value)

# IMPORTANT: The following comments are used by the autodoc functionality when
#            generating the reference doc. Please, keep them updated
#: ::
#:
#:   DEFAULT_CONVERTERS = {
#:       'default': UnicodeConverter,
#:       'string':  UnicodeConverter,
#:       'int':     IntegerConverter
#:   }
#:


DEFAULT_CONVERTERS = {
    'default': UnicodeConverter,
    'string':  UnicodeConverter,
    'int':     IntegerConverter
}


class Map(object):
    """
    A map is an object that contains a number of *routes*, each one matching a
    URL to an action. ``Map objects`` are initialized with an empty list of rules,
    and a set of default converters (see ``DEFAULT_CONVERTERS``). New rules and
    custom converters can be added after initialization, but as a convenience,
    the constructor accepts two optional arguments:

     - ``rules``: a sequence of :any:`Rule` objects
     - ``converters``: custom converters, passed as a dictionary with name of
       the converter type as the key, and the **class** of the converter as the
       value.

    The mapping objects contain also a list of forbidden URLs (starts empty), that
    will stop processing and force an HTTP Forbidden status to be returned.

    .. note::

       More than one rule can be added for the same URL. Only the first match will
       trigger an action, but normally the *method* (GET, POST, ...) is included in
       the matching.
    """
    def __init__(self, rules=None, converters=None):
        self._rules = []
        self.converters = DEFAULT_CONVERTERS.copy()
        if converters is not None:
            for name, convclass in converters.items():
                self.add_converter(name, convclass)
        for rule in rules or ():
            self.add(rule)
        self.forbidden = set()

    def add(self, rule):
        """
        Adds one rule to the internal list. Order matters: rules are processed in
        the same order they have been added.
        """
        rule.compile(self)
        self._rules.append(rule)

    def add_converter(self, name, convclass):
        """
        Adds one name -> ConverterClass mapping
        """
        self.converters[name] = convclass

    def _split_url(self, url):
        return tuple(filter(len, url.split('/')))

    def add_forbidden(self, url):
        """
        Adds one URL to the forbiddin URLs list
        """
        self.forbidden.add(self._split_url(url))

    def is_forbidden(self, url):
        return self._split_url(url) in self.forbidden

    def get_converter(self, variable, converter_name, args, kwargs):
        if converter_name not in self.converters:
            raise LookupError('the converter {!r} does not exist'.format(converter_name))
        return self.converters[converter_name](*args, **kwargs)

    def match(self, path_info, method=None):
        """
        Matches the URL of a query against the internal list of forbidden and accepted
        routes.

        If the URL matches one of the valid routes, the function may either trigger a
        redirection response, or will return a tuple with two elements: a callable (the
        function that will process the query), and a list with variable:value pairs,
        collected by the matching rule object.

        If a match was found with a forbidden URL, a client error will be raised, with
        code ``Return.HTTP_FORBIDDEN``.

        At last, if no match was found, ``None`` will be returned.

        There is an optional argument for this function: ``method``, which accepts a string
        with a method name (eg. ``'GET'``). If ``method`` is left with the default ``None``
        value, the first route with a matching URL will be returned.

        If ``method`` is specified, then the first route with matching URL and method, **or**
        with no specified method, will be returned.

        In the case where ``method`` is specified, and **all** the rules matching the URL
        call for other methods, a client error will be raised, with status code
        ``Return.HTTP_METHOD_NOT_ALLOWED``.
        """
        ctx = get_context()
        found = False
        for rule in self._rules:
            m = rule.match(path_info)
            if m is not None:
                found = True
                if method is not None and rule.methods and method not in rule.methods:
                    # Most probably we want to keep track of this, to raise an exception
                    # if there was a match but no compatible method
                    continue
                ctx.usagelog.this = rule.this
                if rule.redirect_to is not None:
                    ctx.resp.redirect_to(rule.redirect_to)
                elif self.is_forbidden(rule.this):
                    ctx.resp.client_error(Return.HTTP_FORBIDDEN)
                return rule.action, m

        # If we get here and found is True, then it means that there's a route, but the
        # method we used was not allowed
        if found:
            ctx.resp.client_error(Return.HTTP_METHOD_NOT_ALLOWED, 'method not allowed: %s, %s' % (path_info, method))


# *****************
# CUSTOM CONVERTERS

class SelectionConverter(BaseConverter):
    """
    The regular expression for this converter is very simple: just take whatever
    is left from the URL query.

    When a variable is declared along with this converter type, no other variables
    should follow it, because there will be no URL left for them.

    ``SelectionConverter`` accepts some arguments (which should be seen as "selectors"):

    * ``SEL``
    * ``ASSOC``
    * ``NOLNK``
    * ``BONLY``

    Passing no argument is equivalent to passing just ``SEL``. If any other argument
    is passed, then ``SEL`` **must** be included among the arguments. See the
    documentation for ``to_python`` method to learn about their uses.
    """

    regex = '.*'

    def __init__(self, *args):
        if args:
            if 'SEL' not in args:
                raise ValueError("One of the arguments for selection must be 'SEL'")

            # Possible values: SEL, ASSOC, NOLNK, BONLY
            self.res_order = args
        else:
            self.res_order = ['SEL']

    def to_python(self, value):
        """
        Takes the URL fed as input, breaks it into components, and then performs a nomber
        of operations over it. Finally, return a tuple.

        The number of results in the tuple will match the number of *arguments* passed to
        the converter.

        ``SEL``
          This argument is always present. It will get us the result of
          ``getselection(components)``. This is the last action to be performed, as the
          other arguments affect the ``components`` input.
        ``ASSOC``
          Will add a boolean element to the return tuple. If ``associated_calibrations``
          is found in the URL, it will be removed before the return value will be ``True``.
          Otherwise, return ``False``.
        ``NOLNK``
          Also boolean. If ``nolinks`` is found, it will be removed and the return value
          will be ``False``. Otherwise, return ``True``.
        ``BONLY``
          ``True`` if ``body_only`` is present in the URL. ``False`` otherwise.

        The order in which the arguments have been passed to the selector matters to the
        order in which their results are returned in the tuple. So, if we got
        ``<selection:foo>``, then the result is ``(getselection_dictionary,)``; for
        ``<selection(SEL,NOLNK,BONLY):sel,links,body_only)``, we'll get something
        like ``(dict, bool, bool)``; but for ``<selection(ASSOC,SEL):assoc_cals,sel>``
        we'd get ``(bool, dict)``.

        Notice that for all the examples, we've provided as many variable names as
        arguments, to make sure that the mapping between tuple arguments and variables
        is even. This is also why the order of argument matters: it makes easier to
        match the returned values to variable names.
        """
        assoc = False
        links = True
        bonly = False
        things = [v for v in value.split('/') if v != '']
        if 'ASSOC' in self.res_order:
            try:
                things.remove('associated_calibrations')
                assoc = True
            except ValueError:
                pass

        if 'NOLNK' in self.res_order:
            try:
                things.remove('nolinks')
                links = False
            except ValueError:
                pass

        if 'BONLY' in self.res_order:
            try:
                things.remove('body_only')
                bonly = True
            except ValueError:
                pass

        result = []
        for r in self.res_order:
            if r == 'SEL':
                result.append(getselection(things))
            elif r == 'ASSOC':
                result.append(assoc)
            elif r == 'NOLNK':
                result.append(links)
            elif r == 'BONLY':
                result.append(bonly)
        if len(result) == 1:
            return result[0]
        return tuple(result)


class SequenceConverter(BaseConverter):
    """
    The regular expression for this converter is very simple: just take whatever
    is left from the URL query.

    When a variable is declared along with this converter type, no other variables
    should follow it, because there will be no URL left for them.

    ``SequenceConverter`` accepts arguments. If there are any arguments, they are
    interpreted as a set of unique **allowed** values.
    """

    regex = '.*'

    def __init__(self, *args):
        self.allowed = set(args or ())

    def to_python(self, value):
        """
        Returns a list of strings, where each element is one of the components of
        the URL. Ie. if passed ``"/foo/bar/baz/"``, this function will return
        ``['foo', 'bar', 'baz']``.

        If the converter got arguments, and any of the components is not part
        of the set of arguments, this function will raise a ``ValueError``
        exception.
        """
        things = [v for v in value.split('/') if v != '']
        if self.allowed and any(th not in self.allowed for th in things):
            raise ValueError('Illegal values in the URL')

        return things


# END CUSTOM CONVERTERS
# *********************
