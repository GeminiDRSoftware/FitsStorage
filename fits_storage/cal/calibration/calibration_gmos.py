"""
The CalibrationGMOS class

"""

import math

from sqlalchemy import asc, func, Float

from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.gmos import Gmos

from fits_storage.gemini_metadata_utils import UT_DATETIME_SECS_EPOCH

from .calibration import Calibration
from .calibration import not_imaging
from .calibration import not_processed
from .calibration import not_spectroscopy

from fits_storage.gemini_metadata_utils import gmos_dispersion


class CalibrationGMOS(Calibration):
    """
    This class implements a calibration manager for GMOS, a subclass of
    Calibration.

    """
    instrClass = Gmos
    instrDescriptors = (
        'disperser',
        'filter_name',
        'focal_plane_mask',
        'detector_x_bin',
        'detector_y_bin',
        'array_name',
        'amp_read_area',
        'read_speed_setting',
        'gain_setting',
        'nodandshuffle',
        'nod_count',
        'nod_pixels',
        'prepared',
        'overscan_trimmed',
        'overscan_subtracted'
        )

    def set_applicable(self):
        """
        This method determines which calibration types are applicable
        to the target data set, and records the list of applicable
        calibration types in the class applicable variable.
        All this really does is determine whether what calibrations the
        /calibrations feature will look for. Just because a caltype isn't
        applicable doesn't mean you can't ask the calmgr for one.

        """
        self.applicable = []

        if self.descriptors:

            # MASK files do not require anything,
            if self.descriptors['observation_type'] in ('MASK', 'BPM'):
                return

            # PROCESSED_SCIENCE files do not require anything
            if 'PROCESSED_SCIENCE' in self.types:
                return

            # Do BIAS. Most things require Biases.
            require_bias = True

            if self.descriptors['observation_type'] in ('BIAS', 'ARC'):
                # BIASes and ARCs do not require a bias.
                require_bias = False

            elif self.descriptors['observation_class'] in ('acq', 'acqCal'):
                # acq images don't require a BIAS.
                require_bias = False

            elif self.descriptors['detector_roi_setting'] == 'Central Stamp':
                # Anything that's ROI = Central Stamp does not require a bias
                require_bias = False

            if require_bias:
                self.applicable.append('bias')
                self.applicable.append('processed_bias')

            if ((self.descriptors['spectroscopy'] == True) and
                (self.descriptors['observation_type'] == 'FLAT')):

                self.applicable.append('arc')
                self.applicable.append('processed_arc')

            # If it (is spectroscopy) and
            # (is an OBJECT) and
            # (is not a Twilight) and
            # then it needs an arc, flat
            if ((self.descriptors['spectroscopy'] == True) and
                (self.descriptors['observation_type'] == 'OBJECT') and
                (self.descriptors['object'] != 'Twilight')):

                self.applicable.append('arc')
                self.applicable.append('processed_arc')
                self.applicable.append('flat')
                self.applicable.append('processed_flat')

                # and specphot and if it is not a specphot...
                # specphot should be replaced with standard now.
                if 'STANDARD' not in self.types:
                    self.applicable.append('specphot')

                    if self.descriptors['central_wavelength'] is not None:
                        self.applicable.append('processed_standard')
                        self.applicable.append('processed_slitillum')
                        self.applicable.append('slitillum')

            # If it (is imaging) and (is Imaging focal plane mask) and
            # (is an OBJECT) and (is not a Twilight) and is not acq or acqcal
            # ==> needs flats, processed_fringe, processed_standard

            if ((self.descriptors['spectroscopy'] == False) and
                (self.descriptors['focal_plane_mask'] == 'Imaging') and
                (self.descriptors['observation_type'] == 'OBJECT') and
                (self.descriptors['object'] != 'Twilight') and
                (self.descriptors['observation_class'] not in ['acq', 'acqCal'])):

                self.applicable.append('flat')
                self.applicable.append('processed_flat')
                self.applicable.append('processed_fringe')
                if self.descriptors['central_wavelength'] is not None:
                    self.applicable.append('processed_standard')
                # If it's all that and obsclass science, then it needs a photstd
                # need to take care that phot_stds don't require phot_stds for recursion
                if self.descriptors['observation_class'] == 'science':
                    self.applicable.append('photometric_standard')

            # If it (is nod and shuffle) and (is an OBJECT), then it needs a dark
            if ((self.descriptors['nodandshuffle'] == True) and
                (self.descriptors['observation_type'] == 'OBJECT')):
                # Hack - for now, using a datetime filter.  For recent data we don't need darks
                # replace this with a check against the Gmos.detector once we track it
                ut_datetime = None
                if 'ut_datetime' in self.descriptors.keys():
                    ut_datetime = self.descriptors['ut_datetime']
                    if not hasattr(ut_datetime, 'year'):
                        ut_datetime = None
                if ut_datetime is None or ut_datetime.year < 2020:
                    self.applicable.append('dark')
                    self.applicable.append('processed_dark')

            # If it is MOS then it needs a MASK
            if 'MOS' in self.types:
                self.applicable.append('mask')

            # If binning is set, we can use a BPM
            if 'detector_x_bin' in self.descriptors and self.descriptors['detector_x_bin'] \
                    and 'detector_y_bin' in self.descriptors and self.descriptors['detector_y_bin']:
                self.applicable.append('processed_bpm')

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

    @not_imaging
    def arc(self, processed=False, howmany=None):
        """
        This method identifies the best GMOS ARC to use for the target
        dataset.

        This will match on arcs for the same instrument, disperser and filter_name
        with the same x and y binning and a wavelength within 0.001 microns tolerance taken
        within a year of the observation.

        This method will also match on `focal_plane_mask`.  If the `focal_plane_mask` is
        5.0arcsec, this is a special case and will will simply match on any value
        ending in "arcsec".

        Finally, we look for a match on `amp_read_area`.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw arcs
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default 1 arc
        howmany = howmany if howmany else 1
        filters = []
        # Must match focal_plane_mask only if it's not the 5.0arcsec slit in the
        # target, otherwise any longslit is OK
        if self.descriptors['focal_plane_mask'] != '5.0arcsec':
            filters.append(Gmos.focal_plane_mask == self.descriptors['focal_plane_mask'])
        else:
            filters.append(Gmos.focal_plane_mask.like('%arcsec'))

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as
        # all amps must be there - this is more efficient for the DB as it will use
        # the index. Otherwise, the science frame could have a subset of the amps
        # thus we must do the substring match

        # IF looking for processed thing
        #     Full frame is always ok
        #     Central Spectrum can match Central Spectrum as well
        #     Custom detector_roi_
        if processed:
            if self.descriptors['detector_roi_setting'] == 'Full Frame':
                filters.append(Header.detector_roi_setting == 'Full Frame')
            elif self.descriptors['detector_roi_setting'] == 'Central Spectrum':
                filters.append(Header.detector_roi_setting.in_(['Full Frame', 'Central Spectrum']))
            else:
                filters.append(Header.detector_roi_setting == 'Full Frame')
        else:
            if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
                filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
            elif self.descriptors['amp_read_area'] is not None:
                    filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        query = self.get_query() \
                .arc(processed) \
                .add_filters(*filters) \
                .match_descriptors(Header.instrument,
                                   Gmos.disperser,
                                   Gmos.filter_name,    # Must match filter (KR 20100423)
                                   Gmos.detector_x_bin, # Must match ccd binning
                                   Gmos.detector_y_bin) \
                .tolerance(central_wavelength=0.001) \
                .max_interval(days=365)
        return (query.all(howmany))
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

    def dark(self, processed=False, howmany=None):
        """
        Method to find best GMOS Dark frame for the target dataset.

        This will match on darks for the same instrument, read speed, gain, and nod/shuffle
        with the same x and y binning and an exposure time within 50s tolerance taken
        within a year of the observation.

        Finally, we look for a match on `amp_read_area`.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw darks
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 15

        filters = []
        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as
        # all amps must be there - this is more efficient for the DB as it will use
        # the index. Otherwise, the science frame could have a subset of the amps thus
        # we must do the substring match

        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # Must match exposure time. For some strange reason, GMOS exposure times
        # sometimes come out a few 10s of ms different between the darks and science
        # frames

        # K.Roth 20110817 told PH just make it the nearest second, as you can't
        # demand non integer times anyway. Yeah, and GMOS-S ones come out a few 10s
        # of *seconds* different - going to choose darks within 50 secs for now...
        # That's why we're using a tolerance to match the exposure time

        query = \
            self.get_query() \
                .dark(processed) \
                .add_filters(*filters) \
                .match_descriptors(Header.instrument,
                                   Gmos.detector_x_bin,
                                   Gmos.detector_y_bin,
                                   Gmos.read_speed_setting,
                                   Gmos.gain_setting) \
                .tolerance(exposure_time = 50.0) \
                .match_descriptors(Gmos.nod_count, Gmos.nod_pixels) \
                .max_interval(days=365)
        # note, also matched Gmos.nodandshuffle in earlier match_descriptors
        # old variant depended on nodandshuffle, which is not currently a descriptor and only available for calcache/table-to-table
        # .if_(self.descriptors['nodandshuffle'], 'match_descriptors', Gmos.nod_count, Gmos.nod_pixels) \

        return (query.all(howmany))

    def bias(self, processed=False, howmany=None):
        """
        Method to find the best bias frames for the target dataset

        This will match on biases for the same instrument, read speed, and gain
        with the same x and y binning and
        within a 90 days of the observation.

        For `prepared` data, we also look for a match on `overscan_trimmed` and
        `overscan_subtracted`.

        Finally, we look for a match on `amp_read_area`.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw biases
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """

        if howmany is None:
            howmany = 1 if processed else 50

        filters = []
        # The science amp_read_area must be equal or substring of the cal
        # amp_read_area If the science frame uses all the amps, then they must be a
        # direct match as all amps must be there - this is more efficient for the DB
        # as it will use the index. Otherwise, the science frame could have a subset
        # of the amps thus we must do the substring match

        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
            filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # The Overscan section handling: this only applies to processed biases
        # as raw biases will never be overscan trimmed or subtracted, and if they're
        # processing their own biases, they ought to know what they want to do.
        if processed:
            if self.descriptors['prepared'] == True:
                # If the target frame is prepared, then we match the overscan state.
                filters.append(Gmos.overscan_trimmed == self.descriptors['overscan_trimmed'])
                filters.append(Gmos.overscan_subtracted == self.descriptors['overscan_subtracted'])
            else:
                # If the target frame is not prepared, then we don't know what
                # their procesing intentions are. We could go with the default
                # (which is trimmed and subtracted).
                # But actually it's better to just send them what we have, as we has
                # a mishmash of both historically
                #
                #filters.append(Gmos.overscan_trimmed == True)
                #filters.append(Gmos.overscan_subtracted == True)
                pass

        query = self.get_query() \
                .bias(processed) \
                .add_filters(*filters) \
                .match_descriptors(Header.instrument,
                                  Gmos.detector_x_bin,
                                  Gmos.detector_y_bin,
                                  Gmos.read_speed_setting,
                                  Gmos.gain_setting) \
                .max_interval(days=90)
        return (query.all(howmany))

    def bpm(self, processed=False, howmany=None):
        """
        This method identifies the best GMOS BPM to use for the target
        dataset.

        This will match on bpms for the same instrument

        Parameters
        ----------

        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default 1 bpm
        howmany = howmany if howmany else 1

        if self.descriptors['array_name'] is None or self.descriptors['array_name'].strip() == '':
            results = list()
        else:
            filters = [Header.ut_datetime <= self.descriptors['ut_datetime'],
                       Gmos.array_name.like('%' + self.descriptors['array_name'] + '%'),
                       ]
            query = self.get_query() \
                        .bpm(processed) \
                        .add_filters(*filters) \
                        .match_descriptors(Header.instrument,
                                           Gmos.detector_x_bin, # Must match ccd binning
                                           Gmos.detector_y_bin)
            results = query.all(howmany)

        return results

    def imaging_flat(self, processed, howmany, flat_descr, filt):
        """
        Method to find the best imaging flats for the target dataset

        For unprocessed imaging flats, we look for a target type of `Twilight`.

        This will match on imaging flats for the provided `flat_descr` elements
        within 180 days of the observation.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats
        howmany : int, default 1
            How many matches to return
        flat_descr : list of descriptors
            The list of descriptors to match against
        filt : list of filters
            Additional list of filters to apply to the query
        render_query : bool
            If True, retuns the SqlAlchemy query along with the regular return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 20

        if processed:
            query = self.get_query().PROCESSED_FLAT()
        else:
            # Imaging flats are twilight flats
            # Twilight flats are dayCal OBJECT frames with target Twilight
            query = self.get_query().raw().dayCal().OBJECT().object('Twilight')

        query.add_filters(*filt) \
            .match_descriptors(*flat_descr) \
            .max_interval(days=180)
        return (query.all(howmany))

    def spectroscopy_flat(self, processed, howmany, flat_descr, filt):
        """
        Method to find the best spectroscopy flats for the target dataset

        This will match on spectroscopy flats for the provided `flat_descr` elements
        and `filt` filters with a `central_wavelength` tolerance of 0.001 microns
        within 180 days of the observation.  It will also do some fuzzy matching
        on the elevation.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats
        howmany : int, default 1
            How many matches to return
        flat_descr : list of descriptors
            The list of descriptors to match against
        filt : list of filters
            Additional list of filters to apply to the query

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        if howmany is None:
            howmany = 1 if processed else 2

        ifu = mos_or_ls = False
        el_thres = 0.0
        under_85 = False
        crpa_thres = 0.0
        # QAP might not give us these for now. Remove this 'if' later when it does
        if self.descriptors.get('elevation') is not None:
            # Spectroscopy flats also have to somewhat match telescope position
            # for flexure, as follows this is from FitsStorage TRAC #43 discussion
            # with KR 20130425. This code defines the thresholds and the conditions
            # where they apply.

            try:
                ifu = self.descriptors['focal_plane_mask'].startswith('IFU')
                # For IFU, elevation must we within 7.5 degrees
                el_thres = 7.5
            except AttributeError:
                # focal_plane_mask came as None. Leave 'ifu' as False
                pass
            try:
                mos_or_ls = self.descriptors['central_wavelength'] > 0.55 or self.descriptors['disperser'].startswith('R150')
                # For MOS and LS, elevation must we within 15 degrees
                el_thres = 15.0
            except (AttributeError, TypeError):
                # In cases where disperser or central_wavelength is None
                pass
            under_85 = self.descriptors['elevation'] < 85

            # crpa*cos(el) must be within el_thres degrees, ie crpa must be within
            # el_thres / cos(el) when el=90, cos(el) = 0 and the range is infinite.
            # Only check at lower elevations.

            if under_85:
                crpa_thres = el_thres/math.cos(math.radians(self.descriptors['elevation']))

        query = self.get_query() \
                .flat(processed) \
                .add_filters(*filt) \
                .match_descriptors(*flat_descr) \
                .tolerance(central_wavelength=0.001) \
                .tolerance(condition = ifu, elevation=el_thres) \
                .tolerance(condition = mos_or_ls, elevation=el_thres) \
                .tolerance(condition = under_85, cass_rotator_pa=crpa_thres) \
                .max_interval(days=180)
        return (query.all(howmany))
        # return (
        #     self.get_query()
        #         .flat(processed)
        #         .add_filters(*filt)
        #         .match_descriptors(*flat_descr)
        #     # Central wavelength is in microns (by definition in the DB table).
        #         .tolerance(central_wavelength=0.001)
        #
        #     # Spectroscopy flats also have to somewhat match telescope position
        #     # for flexure, as follows this is from FitsStorage TRAC #43 discussion with
        #     # KR 20130425.  See the comments above to explain the thresholds.
        #
        #         .tolerance(condition = ifu, elevation=el_thres)
        #         .tolerance(condition = mos_or_ls, elevation=el_thres)
        #         .tolerance(condition = under_85, cass_rotator_pa=crpa_thres)
        #
        #     # Absolute time separation must be within 6 months
        #         .max_interval(days=180)
        #         .all(howmany)
        #     )

    def flat(self, processed=False, howmany=None):
        """
        Method to find the best GMOS FLAT fields for the target dataset

        This will match on flats for the same instrument, read speed, filter,
        focal plane mask, disperser, amp read area, and gain with the same
        x and y binning.

        It will then do the matching using either :meth:`spectroscopy_flat` or
        :meth:`imaging_flat` as appropriate.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw flats
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        filters = []

        # Common descriptors for both types of flat
        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        flat_descriptors = (
            Header.instrument,
            Gmos.detector_x_bin,
            Gmos.detector_y_bin,
            Gmos.filter_name,
            Gmos.read_speed_setting,
            Gmos.gain_setting,
            Header.spectroscopy,
            # Focal plane mask must match for imaging too... To avoid daytime
            # thru-MOS mask imaging "flats"
            Gmos.focal_plane_mask,
            Gmos.disperser,     # this can be common-mode as imaging is always 'MIRROR'
            )

        # The science amp_read_area must be equal or substring of the cal
        # amp_read_area. If the science frame uses all the amps, then they must
        # be a direct match as all amps must be there - this is more efficient for
        # the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match.

        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            flat_descriptors = flat_descriptors + (Gmos.amp_read_area,)
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        if self.descriptors['spectroscopy']:
            return self.spectroscopy_flat(processed, howmany, flat_descriptors, filters)
        else:
            return self.imaging_flat(processed, howmany, flat_descriptors, filters)

    def fringe(self, processed=False, howmany=None):
        if processed:
            return self.processed_fringe(howmany=howmany)
        else:
            return []

    def processed_fringe(self, howmany=None):
        """
        Method to find the best processed_fringe frame for the target dataset.
        Note that the concept of a raw fringe frame is meaningless.

        This will match on amp read area, filter name, and x and y binning.  It matches
        within 1 year.

        Parameters
        ----------

        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 1

        filters = []
        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as
        # all amps must be there - this is more efficient for the DB as it will use
        # the index. Otherwise, the science frame could have a subset of the amps thus
        # we must do the substring match.

        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                .PROCESSED_FRINGE()
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Gmos.detector_x_bin,
                                   Gmos.detector_y_bin,
                                   Gmos.filter_name)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    def standard(self, processed=False, howmany=None):
        """
        Method to find the best standard frame for the target dataset.

        This will match on flats for the same instrument, read speed, filter,
        focal plane mask, disperser, amp read area, and gain with the same
        x and y binning.

        It will then do the matching using either :meth:`spectroscopy_flat` or
        :meth:`imaging_flat` as appropriate.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw standards
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 1

        filters = []

        query = self.get_query() \
                .standard(processed) \
                .add_filters(*filters) \
                .tolerance(central_wavelength=self._wavelength_tolerance())\
                .match_descriptors(Header.instrument,
                                   Gmos.disperser,
                                   Gmos.detector_x_bin,
                                   Gmos.detector_y_bin,
                                   Gmos.filter_name) \
                .max_interval(days=183)
        results = query.all(howmany)

        return results



    # We don't handle processed ones (yet)
    @not_processed
    @not_imaging
    def specphot(self, processed=False, howmany=None):
        """
        Method to find the best specphot observation

        This will match on non-'Twilight' partner cal or program cal for the
        same instrument, filter, disperser, focal plane mask, and amp read
        area with  a central wavelength within 0.05 (or 0.1 for MOS) microns
        within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw specphots.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # TODO - Deprecate this and ensure "standard" is functionally equivalent
        # in spectroscopy.

        # Default number to associate
        howmany = howmany if howmany else 4

        filters = []

        tol = self._wavelength_tolerance()
        # If the science is MOS or LS, any longslit will do for the specphot.
        # If the science is IFU, must use the same IFO for the specphot.
        if 'MOS' in self.types:
            filters.append(Gmos.focal_plane_mask.contains('arcsec'))
            tol = self._wavelength_tolerance(pixels=1000)
        elif 'LS' in self.types:
            filters.append(Gmos.focal_plane_mask.contains('arcsec'))
        elif 'IFU' in self.types:
            filters.append(Gmos.focal_plane_mask.contains('IFU'))
        else:
            # Catchall for oddball cases, require direct match
            filters.append(Gmos.focal_plane_mask == self.descriptors['focal_plane_mask'])

        # amp_read_area is not relevant to specphots

        query = (self.get_query()
                # They are OBJECT spectroscopy frames
                # with target not twilight.
                # As of DRAGONS 3.2, the 'STANDARD' tag is the definitive way
                # to tell if it is a valid specphot star.
                .raw().OBJECT().spectroscopy(True)
                .add_filters(Header.types.contains('STANDARD'),
                             Header.object != 'Twilight',
                             *filters)
                # Note no requirement to match detector binning
                .match_descriptors(Header.instrument,
                                   Gmos.filter_name,
                                   Gmos.disperser)
                # Must match central wavelength to within some tolerance.
                .tolerance(central_wavelength=tol)
                # Absolute time separation must be within 6 months (KL20250204)
                .max_interval(days=183)
            )

        orderby = self._closest_wlen_time_order(time_range=365,
                                                wlen_range=tol)
        return query.all(howmany, order_by=orderby)

    # We don't handle processed ones (yet)
    @not_processed
    @not_spectroscopy
    def photometric_standard(self, processed=False, howmany=None):
        """
        Method to find the best photometric standard observation

        This will match on partner cal files with a 'CAL' program id and matching filter name
        taken within a day.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw photometric standards.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 4

        return (
            self.get_query()
                # They are OBJECT imaging partnerCal frames taken from CAL program IDs
                .photometric_standard(OBJECT=True, partnerCal=True)
                .add_filters(Header.program_id.like('G_-CAL%'))
                .match_descriptors(Header.instrument,
                                   Gmos.filter_name)
                # Absolute time separation must be within 1 days
                .max_interval(days=1)
                .all(howmany)
            )

    # We don't handle processed ones (yet)
    @not_processed
    def mask(self, processed=False, howmany=None):
        """
        Method to find the best mask

        This will match on GMOS 'MASK' observation type data with a matching focal plane mask.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw masks.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                # They are MASK observation type
                # The focal_plane_mask of the science file must match the
                # data_label of the MASK file (yes, really...)
                # Cant force an instrument match as sometimes it just says GMOS
                # in the mask...

                .add_filters(Header.observation_type == 'MASK',
                             Header.data_label == self.descriptors['focal_plane_mask'],
                             Header.instrument.startswith('GMOS'))
                .all(howmany)
            )

    @not_imaging
    def slitillum(self, processed=False, howmany=None):
        """
        Method to find the best slit response for the target dataset.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw masks.
        howmany : int, default 1
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.core.orm.header.Header` records
            that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 1
        filters = []

        # For slitillum we require the same spatial (y) binning, but no
        # requirement to match spectral (x) binning. (PH discussion with
        # Chris Simpson 20-Apr-2023)

        wlen_tol = self._wavelength_tolerance()
        query = self.get_query() \
                .slitillum(processed) \
                .add_filters(*filters) \
                .tolerance(central_wavelength=wlen_tol)\
                .match_descriptors(Header.instrument,
                                   Gmos.disperser,
                                   Gmos.detector_y_bin,
                                   Gmos.filter_name,
                                   Gmos.focal_plane_mask) \
                .max_interval(days=183)
        orderby = self._closest_wlen_time_order(time_range=183,
                                                wlen_range=wlen_tol)
        return query.all(howmany, order_by=orderby)


    def _wavelength_tolerance(self, pixels=200):
        """
        Calculate a sensible default wavelength tolerance.
        This uses a simplified calculation based on the disperser name to
        estimate the dispersion, and then generate a wavelength tolerance
        from a number of (unbinned) pixels. The default is 200 pixels but you
        can specify that as required.

        Returns
        -------
        float : suggested wavelength tolerance in um
        """
        # Find the dispersion, assume worst case if we can't match it
        dispersion = gmos_dispersion(self.descriptors['disperser'])
        if dispersion is None:
            dispersion = 0.03/1200.0
    
        # Note, dispersion here is the approximate um/pixel.

        # Replace with this if we start storing dispersion in the gmos table
        # and map it in above in the list of `instrDescriptors` to copy.
        # dispersion = float(self.descriptors['dispersion'])

        # OO: per conversation with Chris Simpson
        # PH: Presumably the intent is a 200 pixel tolerance.
        tolerance = pixels * dispersion

        return tolerance

    def _closest_wlen_time_order(self, time_range=1, wlen_range=0.1):
        """
        This utility function generates the argument for an order_by()
        clause that consists of a weighted combination of closest in time
        and closest in wavelength. This is useful for various gmos spectroscopy
        calibrations such as photspec and slitillum where we take them
        infrequently and also sometimes at not quite the same wavelength - for
        example if the science does a wavelength dither to cover the chip gaps
        but the calibration (often) does not. We include a wavelength tolerance
        to account for the latter, but a wavelength tolerance plus closest-in-
        time ordering will often result in the first item returned not being the
        best (ie it is within the tolerance but an exact or much closer
        wavelength match may be available just a few minutes earlier or later).

        Essentially a scoring metric is generated, as follows:
        score = abs(DT/time_range) + abs(DW/wlen_range)
        where:
        DT is DeltaTime = abs(science_utdatetime - calibration_utdatetime)
        DW is DeltaWavelength defined similarly
        time_range is interpreted as being in days
        wlen_range is interpreted as being in microns.

        - ie we normalize the time and wlen deltas each by a nominal range
        in order to weight them in the score appropriately.
        """

        # absolute time separation part first

        # Normalization factor supplied in days, need seconds
        norm_secs = float(time_range * 86400)

        sci_ut_dt_secs = int((self.descriptors['ut_datetime']
                               - UT_DATETIME_SECS_EPOCH).total_seconds())
        dt_score = func.abs(
            func.cast((Header.ut_datetime_secs - sci_ut_dt_secs), Float)
            / norm_secs)
        sci_wl = self.descriptors['central_wavelength']
        wl_score = func.abs(
            func.cast((Header.central_wavelength - sci_wl), Float)
            / wlen_range)

        score = dt_score + wl_score

        # Smallest score is best, sort ascending.
        order = asc(score)

        return order
