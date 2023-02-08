# Adding Search Terms

This document captures what you need to do in the code to add support
for a new search term.

## queryselection_filters

The simplest terms can be captured in the queryselectin_filters
definition in selection.py.  Just map the name you want to give
they key in the URL to the SQLAlchemy field you want it to search
against.  If it is possible to add support in this way, that is
probably the best choice.

Example:

I am adding rawcc support for the SOSes to be able to query against
this term.  I have decided to call it raw_cc in the URL terms.  So
I added 'raw_cc': Header.raw_cc to the queryselection_filters
list of mappings.

## get_selection_key_value

For most of these simple terms, you also need to add a mapping
(even an identity mapping) to get_selection_key_value in
selection.py.