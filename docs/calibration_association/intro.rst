Introduction
============

The ``FitsStorage`` package provides a calibration management subsystem, the
primary function of which is *calibration association*. The concept here is
that the FitsStorage database contains files which may serve as calibrations,
science files (ie a file which may need calibrating) or both (ie a calibration
file may itself need calibrating before use).

``FitsStorage`` and its calibration manager rely heavily on the ``AstroData``
package which form part of the Gemini ``DRAGONS`` system. Basic familiarity
with AstroData concepts such as tags and descriptors is assumed in this
document.

Notionally, one can point to a given science file, and and say "I need a flat
field for that" and the calibration manager will tell you what the best flat
field to use is. Of course, "flat field" is just an example of a *calibration
type* here, and also we note that the science file does not have to actually be
in the fits storage database - if it is not, you have to provide the AstroData
tags and some descriptor values in the calibration request. This enables
requesting calibrations for a science file which is actually an intermediate
product in the middle of pipeline processing, which is important as in the
general case the correct calibration file to use may depend on the processing
history of the intermediate file as the calibration may have to have been
processed in a manner that is compatible with the science file processing.


Terminology
-----------

A note on some of the terms used in this document, and in the FitsStorage code
is helpful here.

science frame (or file)
    We often say "science frame" when actually we simply mean a dataset for
    which a calibration is being requested. Conceptually, this often *is* a
    science frame (e.g. an on-sky frame which you wish to flat field), but in
    reality it can also be a calibration frame that requires a
    calibration of its own. For example if you wish to flat field an Arc lamp
    exposure before fitting the lines in it, you would request a flat field for
    the arc frame, and in that request the arc frame would be considered the
    science frame.

calibration association
    The process of *associating* a calibration frame with a science frame.

calibration frame (or file)
    A calibration file that the calibration manager says is the best match for
    a given calibration association request

calibration type (or caltype)
    All calibration associations must be for a specific calibration type. For
    example Flat Fields are caltype "flat". Arc frames are caltype "arc". Not
    all calibration types are applicable to all science frames - for example
    Arc frames are generally not applicable to Imaging data. While we try to
    define calibration types to be as general as possible, there may be some
    calibration types that are only used by specific instruments.

raw vs processed calibration
    The calibration manager understands the concept of raw calibration files
    and processed calibration files, and is generally capable of associating
    either. Generally, processed calibrations are considered a different caltype
    to raw calibrations, and their caltype starts with ``processed_``. So for
    example a raw flat field frame would be caltype ``flat``, but a processed
    flat field would be a ``processed_flat``. Pipeline reduction systems will
    of course usually request the processed calibration, with the intent of
    applying it directly to the data. Users manually collecting together the
    raw data files they need to process their data would request the raw
    calibrations if they wish to reduce the calibration data themselves, or if
    processed versions are not available.