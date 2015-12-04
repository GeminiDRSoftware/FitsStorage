Application Programming Interface
=================================

.. _authorization_api:

Authorization
-------------

From the point of view of authorization, we classify the users with respect to two
dimensiones. The first one, is the **membership** of the user with respect to the Archive
and Gemini, with increasing access level:

Anonymous user
  This is a user that hasn't logged in
Logged in user
  This is a user of the archive that has an identity in the system and has introduced their
  credentials. There are three roles:

  * **Generic**:
    A normal user of the archive, with interest on browsing data and downloading it (PIs, etc.)
    All logged in users have this role.
  * **Staff member**:
    A Gemini staff member. This category gives access to more functionality, like logs, queue
    status, etc.
  * **Superuser**:
    This status gives access to administrative functionality (user management, data curation,
    etc.)

The other dimension has to do with access to data of metadata and it is associated to the
**ownership of an observing program**. A certain user may have one or more observing programs
associated to itself, resulting in the following access rules:

Anonymous user
  Can access only public pages, and non-proprietary data. In general, anonymous users
  will have access to metadata for any file, whether proprietary or not, except for a
  handful of special cases.
Generic logged in user
  On top of the anonymous user access, a logged in user will have access to proprietary
  data **and** metadata for observing programs that are assigned to them.
Staff member
  The staff member may have programs assigned to them (as any with any user with a login),
  but they will have access to **all** data and metadata, proprietary or not.
Superuser
  Being a superuser does not give special access to data or metadata. It will depend on their
  status as staff.

There are number of functions that can be used to figure out the authorization level
of a user. The low level ones are in the ``web.user`` module:

.. autofunction:: fits_storage.web.user.userfromcookie

.. autofunction:: fits_storage.web.user.is_staffer

``userfromcookie`` abstracts the details of obtaining the user object from the
associated browser bookie. It can be used later with other functions.

``is_staffer`` doesn't need to be passed a user object, as it uses ``userfromcookie``
internally. It is useful for cases when need to distinguish just between staff members
and non-staff members.

There are higher level functions in ``utils.userprogram``. Of interest:

.. autofunction:: fits_storage.utils.userprogram.icanhave

.. autofunction:: fits_storage.utils.userprogram.canhave_coords

.. autofunction:: fits_storage.utils.userprogram.got_magic

``userprogram.icanhave`` is actually a dispatcher that users separate functions for each
kind of item that it can handle. The mapping from object kind and function is the
``icanhave_function`` dictionary. Please, update it to add more functionality:

.. autodata:: fits_storage.utils.userprogram.icanhave_function
   :annotation:
