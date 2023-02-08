*************
Web Interface
*************

.. _mod_python: http://modpython.org
.. _WSGI: https://www.python.org/dev/peps/pep-0333

HTTP Query Dispatch
===================

When migrating from `mod_python`_ to `WSGI`_, as part of the abstraction and redesign
of the http query interface, a "routing" system was introduced. The dispatching for
the WSGI application is defined in the :py:mod:`fits_storage.wsgihandler` module,
in the :py:obj:`url_map` object. This mapping is applied to every query by means
of the :py:func:`get_route` function. It is conceptually much simple than then
previous ``mod_python`` handler, in the sense that the dispatching doesn't need to
handle special cases. All the logic for describing and matching the routes to
handling functions is under :py:mod:`fits_storage.utils.web.routing`.

The entry point for the application is the :py:func:`handler` WSGI application.
This application is wrapped in an instance of
:py:class:`fits_storage.utils.web.wsgi_adapter.ArchiveContextMiddleware`, and
assigned to the :py:obj:`application` variable - this is the object that will
be called by the WSGI server.

The ``ArchiveContextMiddleware`` object wraps the whole application
populating the Context with the adequate Request and Response objects, starting
a database session, and taking care of the clean up and of feeding the response
to the server.

:py:func:`handler` is rather simple: it simply calls a delegate application,
`handle_with_static`, which was created to handle ``/static`` routes.
Such routes SHOULD NOT get to the application in a production environment,
as it should be the job of the web server to handle those files efficiently.
It may be necessary to deal with them when testing the application, though,
for example by running the simple server contained in ``wsgihandler`` itself.
The ``handler`` will capture and handle redirect and client error exceptions.

:py:func:`handle_with_static` is just middleware, too. If the query starts by ``/static/``,
it will attach an open file object to the response, setting the correct content
type using a MIME database. Otherwise, it will let the query pass through to the
`core_handler` function, which is our real application.

:py:func:`core_handler` sets some response headers, and calls py:func:`get_route`
to obtain the function that will handle the query. Then, it either dispatches the
query, or raises a "not found" error.

Routing
=======

.. currentmodule:: fits_storage.utils.web.routing

The routing mechanism is the system that connects HTTP queries to the rest of the
program. In our wsgi handler we do the matching using the :py:func:`get_route`
function. This function takes a :py:class:`Map` object as an argument:

.. autoclass:: Map
   :members:

Before introducing the ``Rule`` class, let's see an example of :any:`Map` in action
in the ``url_map`` that we can find at the WSGI handler:

.. code-block:: python

    url_map = Map([
        # Queries to the root should redirect to a sensible page
        Rule('/', redirect_to=('/searchform' if use_as_archive else '/')),
        Rule('/debug', debugmessage),
        Rule('/content', content),                                      # Database Statistics
        ...
        Rule('/nameresolver/<resolver>/<target>', nameresolver),        # Name resolver proxy
        Rule('/fileontape/<filename>', fileontape),                     # The fileontape handler
        ...
        Rule('/update_headers', update_headers, methods=['POST']),      # JSON RPC dispatcher
        Rule('/ingest_files', ingest_files, methods=['POST']),          # JSON RPC dispatcher
        ...
        Rule('/tapefile/<int:tapewrite_id>', tapefile),                 # TapeFile handler
        Rule('/request_account/<seq_of:things>', request_account),      # new account request
        Rule('/password_reset/<int:userid>/<token>', password_reset),   # account password reset request
        ...
        Rule('/associated_cals/<selection(SEL,NOLNK,BONLY):selection,links,body_only>',
             partial(summary, 'associated_cals'),
             collect_qs_args=dict(orderby='orderby'),
             defaults=dict(orderby=None)),

        ],

        converters = {
            'selection': SelectionConverter,
            'seq_of':    SequenceConverter
        }
    )


An instance of the ``Rule`` class, then, defines an individual matching between
an URL, and some kind of action that will be taken if that URL is matched. The
action may be redirect that will be triggered, or a callable that, when invoked,
will produce the output associated with the URL and the input data.

When specifying a rule, we can match URLs that are fully static, or capture
parts of them into variables. Those variables can be passed just as strings, or
processed according to some kind of "converter". We can provide default values
for some argument names, to adjust for callables that expect more arguments than
variables can be extracted are from the URL. We can specify valid methods,
collect query string arguments, etc. All of this is described in the constructor
for the rule.

.. autoclass:: Rule
   :members:

Converters
----------

Finally, we have the converters. These are some particularly valuable classes,
because they can be used perform data validation during route analysis, and they
can also process input (eg. doing type conversion), decoupling this task from the
actual functions generating the output.

All converters should descend from ``BaseConverter``, or at least follow its model. 

.. autoclass:: BaseConverter
   :undoc-members:
   :members:

When exploring the query URL, the :any:`Rule` class keeps removing the already
recognized elements. When it finds a variable that declares a converter, the matching
alrgorithm will use the converter's regex applying to the remaining URL. If some text
matches, it is extracted and then applied to the ``to_python`` function. If the whole
operation goes without errors or exceptions, the validation is considered successful
and the output of ``to_python`` is assigned as the variable's value.

The way :any:`BaseConverter` is defined, it is guaranteed to succeed.

The routing library comes with a couple of them predefined and incorporated to the,
:any:`Map` class, to provide basic functionality:

.. autodata:: DEFAULT_CONVERTERS
   :annotation:

``UnicodeConverter`` is a dummy derivative of ``BaseConverter`` (essentially, just an
alias), and ``IntegerConverter`` is a very simple class that recognizes numbers and
converts the captured text to Python integer objects. As explained before,
``BaseConverter`` will always succeed when testing a variable's content, and that's
the reason why ``UnicodeConverter`` is the default one.

The WSGI handler provides two more converters, tailored for the Archive application.
First, the :any:`SequenceConverter`, which is used to collect a sequence of URL
components. It replaces the old ``things`` in the mod_python handler:

.. autoclass:: fits_storage.wsgihandler.SequenceConverter
   :undoc-members:
   :members:

The other custom converter is :any:``SelectionConverter``, which can be seen as a
specialized version of the :any:``SequenceConverter``, in that it collects a sequence
of URL components, with the difference that it will interpret them as Archive
"selection" parameters:

.. autoclass:: fits_storage.wsgihandler.SelectionConverter
   :undoc-members:
   :members:
