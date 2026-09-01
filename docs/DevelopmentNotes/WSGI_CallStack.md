In uswgi fitsservice.ini:

`module = fits_storage.server.wsgi.wsgiapp:application`

Alternatively in wsgiapp.py:

`httpd = wsgiref.simple_server.make_server(server, port, application)`

In either case, in wsgiapp.py:

`application = ArchiveContextMiddleware(handler)`

So we pass `handler` to `ArchiveContextMiddleware.__init__()` and the resulting
`ArchiveContextMiddleware` instance *is* the `application`, which is called by 
uwsgi or wsgiref.simple_server.

`handler` is a function defined in wsgiapp.py:

* It tries the static server which handles things like CSS files - in a real 
deployment these should be handled directly by the web server but this allows 
the whole thing to run under wsgiref.simple_server, which is nice for testing.

```
handle_with_static = StaticServer(core_handler)
return handle_with_static(environ, start_response)
```

* If the static server fails - ie for non-static actual content - it calls the
real code (irritatingly via the except clause - can we clean this up?):

`return ctx.resp.respond(unicode_to_string)`

Backing up to `ArchiveContextMiddleware`. We pass it the handler() function
when we instantiate it, it's just stored (as self.application (!)) for later.
All the work here is done in `__call__()`:

* Initialize the context - note that the call to get_context() in handler() will
then get this initialized context.
* get a session from sessionfactory()
* instantiate the Request and Response objects and put them in the context
* initiate a usagelog entry.
* handle blocked requests directly
* actually handle the request:
```
result = self.application(environ, start_response)
return ContextResponseIterator(result, self.close)
```
where `self.application` is the `handler` function discussed earlier, which 
calls `ctx.resp.respond()` (which it can now do because `ArchiveContextMiddleware`
) instantiated a `Response` object as `ctx.resp`.

The `Response` object acts like a buffer. It has an internal `_content` list
which things get appended to, and an `__iter__` method that yields elements of
that list. The `respond()` method returns `iter(self)`.

That is passed through `ContextResponseIterator` which basically provides
a `close()` method that calls `usagelog.set_finals()`, and commits and closes
the database session.

Presumably, this close() method is called by uwsgi or simple_server... ???

