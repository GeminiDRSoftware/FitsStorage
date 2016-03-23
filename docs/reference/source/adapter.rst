***************************************
The WSGI Adapter and the Context Object
***************************************

.. py:currentmodule:: fits_storage.utils.web

The Archive Software uses a WSGI adapter as the interface to the web server.
This adapter gives access to a *Context* and, through it, to the request object
and the response object.

The request object provides information about the query, including contextual
data inferred from HTTP headers and cookies.

The response object provides a way to return content to the client, and to set
HTTP headers (eg. cookies).

Finally, the adapter implements a *routing* mechanism to direct the queries to
the appropriate processing function. This document describes the routing itself,
and how to extend it.

The Adapter Package
===================

All the WSGI adapter functionality is implemented under the
:py:mod:`fits_storage.utils.web` package. Access to the adapter must be done only
using classes and functions exposed by the package. This approach was developed
to ensure a transparent migration from ``mod_python`` to ``WSGI``, and it's not
needed any longer, but the scheme is useful in providing a consistent interface
similar to other WSGI frameworks, and can be used to migrate to other technology
if needed.

.. autofunction:: get_context

.. autoclass:: Return

.. autoexception:: ClientError

.. autoexception:: RequestRedirect

The Context Object
==================

.. autoclass:: fits_storage.utils.web.adapter.Context
   :members:
   :exclude-members: setContent

   .. py:attribute:: req

      The :any:`Request` object instance belonging to this context

   .. py:attribute:: resp

      The :any:`Response` object instance belonging to this context

The Cookies Object
==================

.. autoclass:: fits_storage.utils.web.adapter.Cookies
   :members:

The Request Object
==================

.. autoclass:: fits_storage.utils.web.adapter.Request
   :members:

The Response Object
===================

.. autoclass:: fits_storage.utils.web.adapter.Response
   :members:
