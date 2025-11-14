Metadata to describe reduced data products
==========================================

As we add more reduced data to the Gemini Observatory Archive (GOA) we need
an adequate and well defined set of metadata (ie FITS headers) that describe
the status of the reduction that has been done. This is not intended to be a
data history or provenance (those are indeed described elsewhere in the
metadata), rather the statuses being described here are higher level concepts
such as "This data is science quality".

This set of metadata should be applicable to both automatically reduced data
with GOA, and also user-submitted reduced data, where the “user” in this
context could be an individual PI, but other examples include a survey team,
an instrument team, or an observatory staff member. User submitted data could
have been reduced with observatory provided tools, 3rd party tools, or some
combination of the two.

Drivers
-------

1. We are working towards running DRAGONS automatically within the GOA
context in order to make large numbers of reduced data products available in
the archive.

2. More organized ingestion of the products from manual use of DRAGONS into
the archive. For example high impact or high public interest science where
there is a desire to make reduced data available, or instrument team
generated reduced SV data products.

3. Same as the previous point, but for non-DRAGONS reductions.

4. More and more visiting instrument teams provide reduced data to their
users, by default via their own distribution methods to PIs that mean the
reduced data products are not available to the general community. It would be
beneficial to facilitate doing this via GOA.

5. We have a large amount of legacy reduced data in the GOA, of variable
pedigree. We (and our users) need to be able to distinguish between that and
more modern reduced data produced in a more controlled manner.

Concepts
--------

Some concepts are used in several of the metadata items, or are otherwise
useful to introduce before we go into the metadata items themselves.

Data Processing "Levels"
^^^^^^^^^^^^^^^^^^^^^^^^

In common with many other similar facilities, we introduce the concept of
level numbers to represent the extent of the processing. Higher level numbers
indicate more processing. The exact definitions of the level numbers will be
somewhat instrument and observing mode specific, though they will be kept as
uniform as possible for simplicity . A simple illustrative example would be:

* Level 0: Raw data
* Level 1: Bias subtracted, Flat fielded
* Level 2: Stacked and Mosaiced

This is relevant to the discussion of which “intermediate products” from a
data processing session are stored in the archive. For example with a deep
imaging observation we would obviously want to archive the deep stacked image
that would normally be considered the final product of the reduction, but we
also want to archive a reduced version of each individual image as this would
facilitate things like generating a light curve rather than a deep stack, or
re-stacking the image including only frames which meet a certain image
quality requirement.

There is a balance to be struck here, we do not want to archive the data at
every possible intermediate step (there is little benefit for example in
arching a de-biased but not flat-fielded image). Thus the concept here is
that there would be at most one file in the archive for each level of data
processing. Some levels may not be appropriate for all observing modes and
thus would not be archived.

Science-Quality vs Quick-Look
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We consider two main categories of reduced data, Science-Quality, and
Quick-Look. Note that we do not preclude adding additional categories in the
future should that be desirable. As these terms are used in several metadata
items below, they are being introduced here ahead of time.

We need concise statements on an easily findable page on the website that
describes what we mean by these terms. Initial definitions for consideration
are as follows:

Science-Quality
"""""""""""""""

*   The Data have been reduced by data reduction software which is not
    known to contain any bugs or deficiencies which would significantly affect
    the quality of the reduced data products.

*   The best available calibrations have been applied, and the calibrations
    are also considered Science-Quality.

Science Quality data are intended to be suitable for science use, however in
most cases they have been generated automatically and have not been reviewed
by an expert. There will likely be cases where the automatic reduction does
not provide good results and the onus is on the end user to verify that the
data and reduction meets their requirements. They are reduced in a general
manner which we believe is applicable to most science cases, however some
science cases will require that the data be re-reduced with reduction
techniques specific to the particular scientific use case.

Quick-Look
""""""""""

One or more of the criteria for designating the data as Science-Quality have
not been met.

Quick-Look reduced data are intended to be used to assess simply whether the
data are of interest to the user, in which case re-reduction or manual
processing may be appropriate.

Reviewing
^^^^^^^^^

There is value to an end-user in knowing if the output of an automated
reduction system has been inspected or reviewed by anyone to check for
problems. We do not of course have the resources to review a significant
amount of the pipeline output and so almost all data will **not** be reviewed.
However, there will be cases where someone does look at the reduced data
products, for example SV data or perhaps data submitted by a PI, instrument
team, or survey team.

If reduced data products do get reviewed, we need to record the fact that
data have been reviewed, by whom, and what the results of the review were,
and we need to make that information available for example in archive search
results.


Software Used and externally submitted data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We anticipate that the vast majority of reduced data in the GOA will be
automatically reduced as part of the GOA facility using DRAGONS. As DRAGONS
development continues, improvements are made and new features are added -
this is tracked at a very high level by a DRAGONS version number. We wish to
remain open to other sources of reduced data, most notably, reduced data
products from:

*   Manually initiated DRAGONS sessions, including by observatory staff
    and/or external users. These could involve recipes, primitives or default
    parameters that have been customized.

*   Other packages and user written software. This could include both PI
    written custom scripts, but also things like visitor instrument team and
    survey team pipelines. Obviously, we would require any data submitted to have
    suitable metadata to facilitate archive searches - this data is noted here
    simply as a reminder that we should ensure the metadata being described here
    to describe data reduction is flexible enough to accommodate such data.

*   IRAF and other legacy data products that are already in the archive and
    will need labeling as such.

Metadata / Header keywords and the “parameter space”
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Having briefly introduced the concepts above, we now describe each metadata
item we propose to record. Each of these will be represented by a FITS header
keyword in the PHU of the data file in question.

PROCITNT - Processing Intent [Science-Quality | Quick-Look]
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Was this data processing intended to result in science quality data, or was
it intended to be a quick look reduction? This will be the primary metadata
item that we use to describe reduced data, especially in the case where it
has not been reviewed.

Pragmatically, if DRAGONS was run in SQ mode in the GOA context, for
an observing mode for which we do not know of significant DRAGONS bugs, this
will be Science-Quality. We can elect to set this to Quick-Look for cases
such as newly released modes that do not have adequate testing. We can also
set this to Quick-Look for files from higher Levels (see above) of reduction
- for example we could envisage a spectroscopic observing mode where we are
confident in things like flat fielding and spectrum extraction, so files
corresponding to those processing levels would show Science-Quality
processing intent, but we may not be sufficiently confident in the robustness
of our telluric correction algorithm and thus files from that and subsequent
processing levels would be marked as Quick-Look processing intent.


PROCMODE - Software mode [Science-Quality | sq | Quick-Look| ql]
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

This is somewhat DRAGONS centric, but could be valid for other software too -
was the data processing software configured for science quality or quick look?
This is mainly for internal housekeeping rather than being user-facing. It
would be visible in the data headers, but would not be shown by default in
the archive search results table


PROCSOFT - Software used [DRAGONS | Free form string]
"""""""""""""""""""""""""""""""""""""""""""""""""""""

The name of the software used. This is mainly for internal housekeeping
rather than being user-facing. It would be visible in the data headers, but
would not be shown by default in the archive search results table.

PROCSVER - Software version - [Free form string, for DRAGONS of the form a.b.c]
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Overall release version. We don’t worry about things like custom recipes or
parameters here, this is just a simple version string. If needed, full
details would be in the history in the reduced data file.

This is mainly for internal housekeeping rather than being user-facing. It
would be visible in the data headers, but would not be shown by default in
the archive search results table.


PROCINBY - Processing initiated by - [Free form string, with reserved values]
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Who initiated the reduction (I say who initiated it rather than who did it so
that it is clear that “DRAGONS” or “IRAF” is not the intended answer here).
This could be but would not necessarily be a person's name, rather values
such as: “Gemini Science Staff”, “Gemini Operations Staff”, “Science Program
Investigator (PI)”, “Survey Team”, “Instrument Team”, “GOA Automatic
reduction”, “FIRE”, “ScALES”, etc. Full list of reserved values TBD.

This is mainly for internal housekeeping rather than being user-facing. It
would be visible in the data headers, but would not be shown by default in
the archive search results table.


PROCRVBY - Processing Reviewed by [Optional, Free form string]
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

A missing, empty or null value would indicate that the reduced data products
have not been reviewed.

If they have been reviewed, we would say here by who or what. This may or may
not be a person’s name, it could have values such as: “Gemini Science Staff”,
“Gemini Operations Staff”, “Science Program Investigator (PI)”, “Survey
Team”, “Instrument Team” for example.

This is mainly for internal housekeeping rather than being user-facing. It
would be visible in the data headers, but would not be shown by default in
the archive search results table.


PROCREVW - Processing Review Outcome [Science-Quality | Quick-Look | FAIL]
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The outcome of the review. If the reviewer considers the data to be not
useful (ie junk), it would be ‘FAIL’. If the reviewer considers the data to
be Science Quality or Quick-Look quality, it would have those values.

Science-Quality intent and Science-Quality review outcome would indicate that
the reduction was intended to be science quality and the reviewer agrees that
it is.

Science-Quality intent and Quick-Look review would indicate that the reviewer
has concerns that the data are not actually of science quality.

Note that in principle, all combinations of Processing Intent and Review
Outcome are allowed.  The software will not prevent data that were reduced
with the intent of Quick-Look from being reviewed as Science-Quality. This
would indicate that the reviewer considers the quick-look reduction to have
actually generated science-quality results.

Pragmatically, the GOA (and others) do need a simple way to determine the
status of some data.The baseline in mind here is that the status we would
show would be:

If the data has been reviewed, the Review Outcome. Otherwise (the vast
majority of the time), the Processing Intent.

PROCLEVL - Processing Level [Integer. Blank for undefined]
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

The processing level. See discussion above. 0 => Raw Data. Larger numbers
mean more advanced processing. Exact definitions will be instrument and mode
specific.

PROCTAG - Processing Tag [Gemini assigned string]
"""""""""""""""""""""""""""""""""""""""""""""""""

A Gemini Assigned string value to describe a batch of data reduction. For
example when GOA re-reduces data due to a new version of DRAGONS or some
other change, we need to be able to differentiate between them. Also we need
to be able to differentiate between GOA automatic DRAGONS reductions, user
supplied DRAGONS reductions and, and legacy IRAF reductions by the SOS/DAS etc.

GOA needs to be able to tell from this tag which of the many reductions it
should offer by default in the search results.

Rather than encoding priority etc values into the tag, the tag is just that -
a label, and the processing_tag table describes their properties.

The Processing Tag is also used as the path (or key prefix in the case of S3)
for where to store the files. Filenames have to be unique within a processing
tag.

Example processing tag names:

IRAF_BIAS: Legacy DAS / SOS IRAF reduced GMOS BIASes

IRAF_FLAT: Legact DAS / SOS IRAF reduced GMOS Twilight flats

GOA_DRAGONS_3.2_GMOS_BIAS: GMOS BIASes reduced by GOA with DRAGONS 3.2

USNGO_GHOST: USGNO GHOST reductions

IGRINS2_PLP:  IGRINS-2 PLP data products

Processing Tag Metadata, searching and calibration association
--------------------------------------------------------------

Metadata regarding the processing tags is stored in the processing_tag table,
and there is a processing_tags.py script to aid in manipulating it. The
processing tag of an individual reduced data file is given in the processing_tag
column of the Header table entry for that file. There is no database schema
foreign key relationship between this and the processing_tags table, which
allows data to be ingested even if it has a processing_tag that is not in the
processing_tag table, however it will not show up by default in search queries
until this is added.

The metadata for each processing tag stored in the processing_tag table are:

tag: the tag name - matched against Header.processing_tag

domain: The domain to which this tag applies. For example GMOS_BIAS,
GMOS_IMAGING, GMOS_LONGSLIT, GHOST, etc. Domains must be non-overlapping
(hence separating GMOS_BIAS from GMOS_IMAGING and GMOS_LONGSLIT). The rationale
for this should become clear when we discuss search results

priority: An integer priority (higher is better) of this tag.

published: Boolean to say of this tag is "published". Tags that are not
published will still be visible in the archive, but will never be selected by
default and will never be used in calibration association.

Searches for reduced data
"""""""""""""""""""""""""

Individual processing tags can be specified in the selection criteria of a
search. If there is no processing tag specification in the selection, no
search filtering is done by processing tag and all processed data will show up.
This, however is rarely the case, as the default search criteria specify
searching on the special processing tag value of "default".

When we get a searchform (or other) search for the "default" processing tag
value, we first search the database with no processing tag search constraints,
to generate a list of processing tags relevant to the other search criteria
given. This is the available tags list and is used to populate the processing
tags pulldown in the search form with the search results. This allows the user
to subsequently re-run the search with any applicable processing tag.

We then look at the metadata of the available tags and generate a list of tags
containing the highest priority tag(s) for each domain, and exluding tags that
are marked as "not published". This is the default tags list, and if the search
is for the "default" processing tag, we include results from all the tags on
this list.

Note that if there are several tags with the same domain and the same
priority value, and that priority value is the maximum for that domain, they
will all get included in the default tags list. This allows us to keep tags
specific to a given set or batch of reduced data, but to include several of
those tags in the default tag list. For example with a GMOS-N_IMAGING domain, we
can have separate tags for twilight flats and science data, and if we regenerate
a new version of the science data and want to keep the flats with it, we can
simply adjust the priority of the flats to the same value as the new science
reduction.

In summary - by leaving the setting at "Default", users will see the highest
priority, "published", reduced data applicable, and they'll get a list of all
available tags in the pulldown if they want to search for a specific one.

Calibration Association
"""""""""""""""""""""""

The current implementation of the calibration association makes it difficult to
implement the same strategy in the calibration manager. This could be added
in future, but for now, when searching for processed calibrations, the
calibration manager will only return results having "published" reduction tags,
and will simply sort results in descending priority order. Given that for most
processed calibrations, only one file is requested, the result will be from the
highest priority tag available.


Retention of superseded data products
-------------------------------------

We need to decide a policy on retention of superseded data products. This
still needs thought.

A simple example is when we re-reduce data with a new software version, or we
find out that a calibration used in reduction was bad and we re-reduced the
data without it. Pragmatically, the simple solution is to just replace old
with new and delete the old. A purist would argue that once data has been
made public it should be forever available so that if it was used in a
publication, someone can fetch that old version to reproduce the results and
then determine if the new version leads to the same results. I think we need
to be pragmatic here, if we keep absolutely every version, this would result
in an explosion in data volume, storage costs, and clutter in user interface
- even if we implement a pull down with reduction versions, we do not want an
excessive number of options in the pulldown.

A less simple example though is for example if there are PI / Instrument team
/ etc reductions of some data and GOA also automatically reduces that data.
In that case I would think that we probably do want to keep both. There may
be cases where both are marked with the same processing intent etc, we should
not use that to differentiate.

The reduction tag value will be a major part of this - we could for example
replace data if the new version has the same LABEL field in the reduction
tab, and retain the old data if the new version has a different LABEL. We
could also have a field in the reduction tag (or indeed a separate header
item) that indicates that these data are always to be retained.

