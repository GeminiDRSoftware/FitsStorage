*************
Web Interface
*************

.. _mod_python: http://modpython.org
.. _Jinja2: http://jinja.pocoo.org
.. _custom filters: http://jinja.pocoo.org/docs/dev/api/#custom-filters
.. _custom tests: http://jinja.pocoo.org/docs/dev/api/#custom-tests
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

Templating
==========

Our tool for templating is `Jinja2`_. It provides a number of functions (filters), sentences, etc.
that are available to all templates as part of their standard library, to make it easier to handle
the data that will be displayed, while limiting the code that goes into a template. As a general
rule, do as much computation as possible in the functions that process the query, and try to make
as little as possible in the templates themselves.

To use the templates from our code, we have created some conventions to hide away the details of
dealing with Jinja from Python. We provide a decorator for functions that return content generated
by a template.

This is the template decorator's signature. Below we show use examples.

.. autofunction:: fits_storage.web.templating.templated

Usually, you'd only need to import the decorator and apply it to the function indicating the path
to the template, which is the only required argument:

.. code-block:: python

    from fits_storage.web import templating

    @templating.templated("path/to/template.html")
    def my_function():
        # Compute the results
        context = {
            'foo': bar,
            'baz': something
            }
        return context

Where context is a dictionary of Python objects. Each key in the dictionary is a variable name
accessible from the template's sentences and expressions. The decorator will take care of invoking
Jinja to generate the code, and to send it as the query output.

Non-200 Status
--------------

By default, all queries return an HTTP `200 OK` status code. If a function associated to a template
needs to indicate something different, then the return value must be a tuple, instead of a dictionary:

.. code-block:: python

    @templating.templated("path/to/template.html")
    def my_function():
        # Compute the results
        return (status, context)

In most cases, though, when returning a different code, we don't need to provide a context: we will let
the web server (or some other piece of our software) to produce the appropriate content. For this cases
we will typically use either :any:`Response.client_error` or :any:`Response.redirect_to`.

Template Context
----------------

While the functionality provided by Jinja is, quite often, more than enough, we find occasionally the
need to extend it. Jinja allows us to create `custom filters`_, `custom tests`_ (used mainly in ``if``
sentences), and even to write extensions that add sentences to the templating language.

To generate a template, the first step is to create your Jinja environment and (optionally) customize.
We do this in the :py:func:`templating.get_env` function. ``get_env`` makes use of the following
structures that you can modify to extend functionality:

.. py:data:: templating.custom_filters

   A dictionary that associates custom filter functions to names that will be accesible to all templates.

.. py:data:: templating.global_members

   A dictionary that associates values to names that will be accessible to all templates.

.. py:data:: templating.included_extensions

   A list of extension modules that we want to enable in the templating engine. Eg. ``'jinja2.ext.with_'``

Access Control
==============

There are web resources in the Archive that are subject to access authorization. These include:

* Administrative pages, only for superusers
* Staff-only pages
* Data and metadata under proprietary rights

The restricted resources may have different granularity: some times one needs a certain level of
authorization to see a page; for other content, lack of authorization only means a degraded service
(ie. some data will be shown, other will be unaccessible or hidden).

Coarse granularity access control (ie., to a whole page) is granted by using a decorator:

.. autofunction:: fits_storage.web.user.needs_login

So, for example, if we would like to restrict a web page to only Staff members, we would write:

.. code-block:: python

    from fits_storage.web.user import needs_login

    @needs_login(staffer = True)
    def my_function():
       # process…

The :py:func:`needs_login` decorator can be combined with others, like in the following example:

.. code-block:: python

    from fits_storage.web.user import needs_login
    from fits_storage.web import templating

    @needs_login(staffer = True)
    @templating.templated("my_template.html")
    def my_function():
       # process…

In this case, the order of the decorators matter, though, because ``templated`` may modify the
number or the order of the arguments passed to the function. Thus, ``needs_login`` should always
be listed first.

If we need more granularity, this "all or nothing" approach is not enough. Instead, we would need
to use some of the internal API functions to figure out the user permissions, and customize the
output based on that. See the :ref:`authorization_api` API section.
