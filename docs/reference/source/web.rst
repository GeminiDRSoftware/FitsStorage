*************
Web Interface
*************

.. _mod_python: http://modpython.org
.. _Jinja2: http://jinja.pocoo.org
.. _custom filters: http://jinja.pocoo.org/docs/dev/api/#custom-filters
.. _custom tests: http://jinja.pocoo.org/docs/dev/api/#custom-tests

HTTP Query Dispatch
===================

The Archive has been built around `mod_python`_. The dispatching is done within
the :py:mod:`fits_storage.apachehandler` module, in the :py:func:`handler` and
:py:func:`thehandler` functions. :py:func:`handler` is the top level entry point
(mandated by mod_python's interface), which takes care of some low level handling,
like capturing certain exceptions, doing some error logging, and generally trying
to make sure that Apache gets back a proper response. It calls thehandler to do the
real dispatching.

In :py:func:`thehandler`, apart from setting a few common headers, we can see
handling of certain queries that need some kind of special preprocessing, like
``/summary``, ``/searchresults``, etc. E.g:

.. code-block:: python

    # Archive searchform
    if this == 'searchform':
        return searchform(req, things, orderby)

    # This is the header summary handler
    if this in {'summary', 'diskfiles', 'ssummary', 'lsummary',
                'searchresults', 'associated_cals'}:
        # the nolinks feature is used especially in external email notifications
        try:
            things.remove('nolinks')
            links = False
        except ValueError:
            links = True
        # the body_only feature is used when embedding the summary
        try:
            things.remove('body_only')
            body_only = True
        except ValueError:
            body_only = False

        # Parse the rest of the uri here while we're at it
        # Expect some combination of program_id, observation_id, date and instrument name
        # We put the ones we got in a dictionary
        selection = getselection(things)

        retval = summary(req, this, selection, orderby, links=links, body_only=body_only)
        return retval

These special-needs queries are exceptions. The rest of the queries are handled by code like this:

.. code-block:: python

    if this in mapping_simple:
        return mapping_simple[this](req)
    if this in mapping_things:
        return mapping_things[this](req, things)
    if this in mapping_selection:
        return mapping_selection[this](req, getselection(things))

Thus, whenever we need to add a new query function, we have to decide whether it falls in one
of the three generic categories, and add the corresponding callable in the matching dictionary.
The categories are:

**simple queries**
  these are mapped in ``apachehandler.mapping_simple``. The callables are passed only one
  argument (the request object). Any other elements in the URI are discarded. If there’s a need
  for further processing, the dispatching function can obtain the URI from the request object or
  be moved to other category.
**queries that expect extra URI elements** 
  These are mapped in ``apachehandler.mapping_things``. The callables
  are passed two arguments: the *request object*, and a list of splitted URI elements, except for the
  first. Eg., if we receive a query for "/usagedetails/1234/683/123", the second argument passed to
  the dispatching function will be a list ``["1234", "683", "123"]``.
**queries that expect a processed list of arguments**
  The list of arguments is known as the *selection*. These functions
  treated almost like ones in the previous category, except that instead of just a list of the raw
  values of the URI elements, the functions receive a dictionary with a processed version. They
  are mapped in ``apachehandler.mapping_selection``.

.. note:: In some cases we’ve decided to use the same entry point for different functions,
  and the callable getting the query will, in turn, act as a second level dispatcher.
  For an example of this, see the :py:mod:`miscfiles` case, where we use a single entry point
  ("/miscfiles") for a number of functions, depending on the desired action. Some of them
  require extra URI parts, some of them won’t. The main handler let’s the more specialized
  handler in the module to take care.

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
    def my_function(req, things):
        # Compute the results
        context = {
            'foo': bar,
            'baz': something
            }
        return context

Where context is a dictionary of Python objects. Each key in the dictionary is a variable name
accessible from the template's sentences and expressions. The decorator will take care of invoking
Jinja to generate the code, and to send it as the query output.  Another common use of the templates
is as follows:

.. code-block:: python

    @templating.templated("path/to/template.html", with_session=True)
    def my_function(session, req, things):
        # Compute the results
        return context

In this case we're asking the decorator to start a new ORM session before invoking our code.
This is not needed at all if our returned context contains only objects unrelated to the ORM
(regular strings, integers, etc., but also complex instances.) If we're returning objects that have
been obtained from a session (eg., a query that will be iterated from inside the template), then we
must keep the session open until all the content has been produced. The decorator takes care of this
for us. It will properly commit/rollback the session at the end.

Non-200 Status
--------------

By default, all queries return an HTTP `200 OK` status code. If a function associated to a template
needs to indicate something different, then the return value must be a tuple, instead of a dictionary:

.. code-block:: python

    @templating.templated("path/to/template.html")
    def my_function(req, things):
        # Compute the results
        return (status, context)

In most cases, though, when returning a different code, we don't need to provide a context: we will let
the web server (or some other piece of our software) to produce the appropriate content. For this cases
we provide an exception:

.. autoexception:: fits_storage.web.templating.SkipTemplateError

To be used like this:

.. code-block:: python

   raise templating.SkipTemplateError(HTTP_NOT_ACCEPTABLE)

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
    def my_function(req):
       # process…

The :py:func:`needs_login` decorator can be combined with others, like in the following example:

.. code-block:: python

    from fits_storage.web.user import needs_login
    from fits_storage.web import templating

    @needs_login(staffer = True)
    @templating.templated("my_template.html")
    def my_function(req):
       # process…

In this case, the order of the decorators matter, though, because ``templated`` may modify the
number or the order of the arguments passed to the function. Thus, ``needs_login`` should always
be listed first.

If we need more granularity, this "all or nothing" approach is not enough. Instead, we would need
to use some of the internal API functions to figure out the user permissions, and customize the
output based on that. See the :ref:`authorization_api` API section.
