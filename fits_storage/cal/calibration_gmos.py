"""
This module holds the CalibrationGMOS class
"""
import datetime

from ..fits_storage_config import using_sqlite
from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.gmos import Gmos
from .calibration import Calibration, not_processed, not_imaging, not_spectroscopy

from sqlalchemy.orm import join
from sqlalchemy import func, extract

import math

class CalibrationGMOS(Calibration):
    """
    This class implements a calibration manager for GMOS.
    It is a subclass of Calibration
    """
    gmos = None
    instrClass = Gmos

    def __init__(self, session, header, descriptors, types):
        """
        This is the init method for the GMOS calibration subclass.
        """
        # Init the superclass
        Calibration.__init__(self, session, header, descriptors, types)

        # if header based, Find the gmosheader
        if header:
            query = session.query(Gmos).filter(Gmos.header_id == self.descriptors['header_id'])
            self.gmos = query.first()

        # Populate the descriptors dictionary for GMOS
        if self.from_descriptors:
            self.descriptors['disperser'] = self.gmos.disperser
            self.descriptors['filter_name'] = self.gmos.filter_name
            self.descriptors['focal_plane_mask'] = self.gmos.focal_plane_mask
            self.descriptors['detector_x_bin'] = self.gmos.detector_x_bin
            self.descriptors['detector_y_bin'] = self.gmos.detector_y_bin
            self.descriptors['amp_read_area'] = self.gmos.amp_read_area
            self.descriptors['read_speed_setting'] = self.gmos.read_speed_setting
            self.descriptors['gain_setting'] = self.gmos.gain_setting
            self.descriptors['nodandshuffle'] = self.gmos.nodandshuffle
            self.descriptors['nod_count'] = self.gmos.nod_count
            self.descriptors['nod_pixels'] = self.gmos.nod_pixels
            self.descriptors['prepared'] = self.gmos.prepared
            self.descriptors['overscan_trimmed'] = self.gmos.overscan_trimmed
            self.descriptors['overscan_subtracted'] = self.gmos.overscan_subtracted

        # Set the list of applicable calibrations
        self.set_applicable()

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
            if self.descriptors['observation_type'] == 'MASK':
                return

            # Do BIAS. Most things require Biases.
            require_bias = True
            if self.descriptors['observation_type'] == 'BIAS':
                # BIASes do not require a bias.
                require_bias = False

            elif self.descriptors['detector_roi_setting'] == 'Custom' and self.descriptors['observation_class'] == 'acq':
                # Custom ROI acq images (efficient MOS acquisitions) don't require a BIAS.
                require_bias = False

            elif self.descriptors['detector_roi_setting'] == 'Central Stamp':
                # Anything that's ROI = Central Stamp does not require a bias
                require_bias = False

            if require_bias:
                self.applicable.append('bias')
                self.applicable.append('processed_bias')

            # If it (is spectroscopy) and
            # (is an OBJECT) and
            # (is not a Twilight)
            # then it needs an arc, flat, spectwilight, specphot
            if ((self.descriptors['spectroscopy'] == True) and
                    (self.descriptors['observation_type'] == 'OBJECT') and
                    (self.descriptors['object'] != 'Twilight')):
                self.applicable.append('arc')
                self.applicable.append('processed_arc')
                self.applicable.append('flat')
                self.applicable.append('processed_flat')
                self.applicable.append('spectwilight')
                self.applicable.append('specphot')


            # If it (is imaging) and
            # (is Imaging focal plane mask) and
            # (is an OBJECT) and (is not a Twilight) and
            # is not acq or acqcal
            # then it needs flats, processed_fringe
            if ((self.descriptors['spectroscopy'] == False) and
                     (self.descriptors['focal_plane_mask'] == 'Imaging') and
                     (self.descriptors['observation_type'] == 'OBJECT') and
                     (self.descriptors['object'] != 'Twilight') and
                     (self.descriptors['observation_class'] not in ['acq', 'acqCal'])):

                self.applicable.append('flat')
                self.applicable.append('processed_flat')
                self.applicable.append('processed_fringe')
                # If it's all that and obsclass science, then it needs a photstd
                # need to take care that phot_stds don't require phot_stds for recursion
                if self.descriptors['observation_class'] == 'science':
                    self.applicable.append('photometric_standard')

            # If it (is nod and shuffle) and
            # (is an OBJECT), then it needs a dark
            if ((self.descriptors['nodandshuffle'] == True) and
                    (self.descriptors['observation_type'] == 'OBJECT')):
                self.applicable.append('dark')
                self.applicable.append('processed_dark')

    @not_imaging
    def arc(self, processed=False, howmany=None):
        """
        This method identifies the best GMOS ARC to use for the target
        dataset.
        """
        # Default 1 arc
        howmany = howmany if howmany else 1
        filters = []
        # Must match focal_plane_mask only if it's not the 5.0arcsec slit in the target, otherwise any longslit is OK
        if self.descriptors['focal_plane_mask'] != '5.0arcsec':
            filters.append(Gmos.focal_plane_mask == self.descriptors['focal_plane_mask'])
        else:
            filters.append(Gmos.focal_plane_mask.like('%arcsec'))

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
            filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                .arc(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Gmos.disperser,
                                   Gmos.filter_name,    # Must match filter (from KR 20100423)
                                   Gmos.detector_x_bin, # Must match ccd binning
                                   Gmos.detector_y_bin)
                .tolerance(central_wavelength=0.001)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .limit(howmany)
                .all()
            )

    def dark(self, processed=False, howmany=None):
        """
        Method to find best GMOS Dark frame for the target dataset.
        """
        if howmany is None:
            howmany = 1 if processed else 15

        filters = []
        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
            filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # Must match exposure time. For some strange reason, GMOS exposure times sometimes come out
        # a few 10s of ms different between the darks and science frames

        # K.Roth 20110817 told PH just make it the nearest second, as you can't demand non integer times anyway.
        # Yeah, and GMOS-S ones come out a few 10s of *seconds* different - going to choose darks within 50 secs for now...
        # That's why we're using a tolerance to match the exposure time

        return (
            self.get_query()
                .dark(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Gmos.detector_x_bin,
                                   Gmos.detector_y_bin,
                                   Gmos.read_speed_setting,
                                   Gmos.gain_setting,
                                   Gmos.nodandshuffle)
                .tolerance(exposure_time = 50.0)
                .if_(self.descriptors['nodandshuffle'], 'match_descriptors', Gmos.nod_count, Gmos.nod_pixels)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .limit(howmany)
                .all()
            )

    def bias(self, processed=False, howmany=None):
        """
        Method to find the best bias frames for the target dataset
        """
        if howmany is None:
            howmany = 1 if processed else 50

        filters = []
        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
            # Can't do a contains if the value is None...
            filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # The Overscan section handling: this only applies to processed biases
        # as raw biases will never be overscan trimmed or subtracted, and if they're
        # processing their own biases, they ought to know what they want to do.

        # If the target frame is prepared, then we match the overscan state. If the target frame is
        # not prepared, then we don't know what thier procesing intentions are, so we
        # give them the default (which is trimmed and subtracted).
        if processed:
            if self.descriptors['prepared'] == True:
                filters.append(Gmos.overscan_trimmed == self.descriptors['overscan_trimmed'])
                filters.append(Gmos.overscan_subtracted == self.descriptors['overscan_subtracted'])
            else:
                filters.append(Gmos.overscan_trimmed == True)
                filters.append(Gmos.overscan_subtracted == True)

        return (
            self.get_query()
                .bias(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                  Gmos.detector_x_bin,
                                  Gmos.detector_y_bin,
                                  Gmos.read_speed_setting,
                                  Gmos.gain_setting)
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .limit(howmany)
                .all()
            )

    # TODO: Discuss 'flat' with Paul. We need to know how the algorithmic paths
    #       a really done in here. Looks quite messy...
    def flat(self, processed=False, howmany=None):
        """
        Method to find the best GMOS FLAT fields for the target dataset
        """
        query = self.session.query(Header).select_from(join(join(Gmos, Header), DiskFile))

        if processed:
            query = query.filter(Header.reduction == 'PROCESSED_FLAT')
            # Default number of processed flats
            howmany = howmany if howmany else 1
        else:
            query = query.filter(Header.reduction == 'RAW')
            # Set default number of raw flats later depending on if spectroscopy

        # Only spectroscopy flats are actually obstype flat for gmos
        # Imaging flats are twilight flats
        if self.descriptors['spectroscopy']:
            query = query.filter(Header.observation_type == 'FLAT')
            # Default number of spectroscopy flats
            howmany = howmany if howmany else 2
        else:
            # Twilight flats are dayCal OBJECT frames with target Twilight
            query = query.filter(Header.observation_class == 'dayCal').filter(Header.observation_type == 'OBJECT')
            query = query.filter(Header.object == 'Twilight')
            # Default number of raw twilight imaging flats
            howmany = howmany if howmany else 20

        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        query = query.filter(Header.instrument == self.descriptors['instrument'])
        query = query.filter(Gmos.detector_x_bin == self.descriptors['detector_x_bin'])
        query = query.filter(Gmos.detector_y_bin == self.descriptors['detector_y_bin'])
        query = query.filter(Gmos.filter_name == self.descriptors['filter_name'])
        query = query.filter(Gmos.read_speed_setting == self.descriptors['read_speed_setting'])
        query = query.filter(Gmos.gain_setting == self.descriptors['gain_setting'])
        query = query.filter(Header.spectroscopy == self.descriptors['spectroscopy'])

        # Focal plane mask must match for imaging too... To avoid daytime thru-MOS mask imaging "flats"
        query = query.filter(Gmos.focal_plane_mask == self.descriptors['focal_plane_mask'])

        if self.descriptors['spectroscopy']:
            query = query.filter(Gmos.disperser == self.descriptors['disperser'])
            # Central wavelength is in microns (by definition in the DB table).
            cenwlen_lo = float(self.descriptors['central_wavelength']) - 0.001
            cenwlen_hi = float(self.descriptors['central_wavelength']) + 0.001
            query = query.filter(Header.central_wavelength > cenwlen_lo).filter(Header.central_wavelength < cenwlen_hi)

            # Spectroscopy flats also have to somewhat match telescope position for flexure, as follows
            # this is from FitsStorage TRAC #43 discussion with KR 20130425
            # QAP might not give us these for now. Remove this 'if' later when it does
            if 'elevation' in self.descriptors:
                if self.descriptors['focal_plane_mask'].startswith('IFU'):
                    # For IFU, elevation must we within 7.5 degrees
                    el_thresh = 7.5 # degrees
                    el_hi = float(self.descriptors['elevation']) + el_thresh
                    el_lo = float(self.descriptors['elevation']) - el_thresh
                    query = query.filter(Header.elevation > el_lo).filter(Header.elevation < el_hi)
                    # and crpa*cos(el) must be within 7.5 degrees, ie crpa must be within 7.5 / cos(el)
                    # when el=90, cos(el) = 0 and the range is infinite. Only check at lower elevations
                    if self.descriptors['elevation'] < 85:
                        crpa_thresh = 7.5 / math.cos(math.radians(self.descriptors['elevation']))
                        crpa_hi = float(self.descriptors['cass_rotator_pa']) + crpa_thresh
                        crpa_lo = float(self.descriptors['cass_rotator_pa']) - crpa_thresh
                        # Should deal with wrap properly here, but need to figure out what we get in the headers round the wrap
                        # simple case will be fine unless they did an unwrap between the science and the flat
                        query = query.filter(Header.cass_rotator_pa > crpa_lo).filter(Header.cass_rotator_pa < crpa_hi)
                else:
                    # MOS or LS case (spectroscopy byt not IFU)
                    if self.descriptors['central_wavelength'] > 0.55 or self.descriptors['disperser'].startswith('R150'):
                        # Elevation must be within 15 degrees
                        el_thresh = 15.0
                        el_hi = float(self.descriptors['elevation']) + el_thresh
                        el_lo = float(self.descriptors['elevation']) - el_thresh
                        query = query.filter(Header.elevation > el_lo).filter(Header.elevation < el_hi)
                        # And crpa*col(el) must be within 15 degrees
                        if self.descriptors['elevation'] < 85:
                            crpa_thresh = 7.5 / math.cos(math.radians(self.descriptors['elevation']))
                            crpa_hi = float(self.descriptors['cass_rotator_pa']) + crpa_thresh
                            crpa_lo = float(self.descriptors['cass_rotator_pa']) - crpa_thresh
                            # Should deal with wrap properly here, but need to figure out what we get in the headers round the wrap
                            # simple case will be fine unless they did an unwrap between the science and the flat
                            query = query.filter(Header.cass_rotator_pa > crpa_lo).filter(Header.cass_rotator_pa < crpa_hi)
                    else:
                        # In this case, no elevation or crpa constraints.
                        pass

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            query = query.filter(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
            query = query.filter(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        # Absolute time separation must be within 6 months
        query = self.set_common_cals_filter(query, max_interval=datetime.timedelta(days=180), limit=howmany)

        return query.all()

    def processed_fringe(self, howmany=None):
        """
        Method to find the best processed_fringe frame for the target dataset.
        Note that the concept of a raw fringe frame is meaningless.
        """
        # Default number to associate
        howmany = howmany if howmany else 1

        filters = []
        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
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
                .limit(howmany)
                .all()
            )

    # We don't handle processed ones (yet)
    @not_processed
    @not_imaging
    def spectwilight(self, processed=False, howmany=None):
        """
        Method to find the best spectwilight - ie spectroscopy twilight
        ie MOS / IFU / LS twilight
        """
        # Default number to associate
        howmany = howmany if howmany else 2

        filters = []
        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
            filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                # They are OBJECT spectroscopy frames with target twilight
                .raw().OBJECT().spectroscopy(True).object('Twilight')
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Gmos.detector_x_bin,
                                   Gmos.detector_y_bin,
                                   Gmos.filter_name,
                                   Gmos.disperser,
                                   Gmos.focal_plane_mask)
                # Must match central wavelength to within some tolerance.
                # We don't do separate ones for dithers in wavelength?
                # tolerance = 0.02 microns
                .tolerance(central_wavelength=0.02)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .limit(howmany)
                .all()
            )

    # We don't handle processed ones (yet)
    @not_processed
    @not_imaging
    def specphot(self, processed=False, howmany=None):
        """
        Method to find the best specphot observation
        """
        # Default number to associate
        howmany = howmany if howmany else 4

        filters = []
        # Must match the focal plane mask, unless the science is a mos mask in which case the specphot is longslit
        if 'MOS' in self.types:
            filters.append(Gmos.focal_plane_mask.contains('arcsec'))
            tol = 0.10 # microns
        else:
            filters.append(Gmos.focal_plane_mask == self.descriptors['focal_plane_mask'])
            tol = 0.05 # microns

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Gmos.amp_read_area == self.descriptors['amp_read_area'])
        else:
            filters.append(Gmos.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                # They are OBJECT partnerCal or progCal spectroscopy frames with target not twilight
                .raw().OBJECT().spectroscopy(True)
                .add_filters(Header.observation_class.in_(['partnerCal', 'progCal']),
                             Header.object != 'Twilight',
                             *filters)
                # Found lots of examples where detector binning does not match, so we're not adding those
                .match_descriptors(Header.instrument,
                                   Gmos.filter_name,
                                   Gmos.disperser)
                # Must match central wavelength to within some tolerance.
                # We don't do separate ones for dithers in wavelength?
                .tolerance(central_wavelength=tol)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .limit()
                .all()
            )

    # We don't handle processed ones (yet)
    @not_processed
    @not_spectroscopy
    def photometric_standard(self, processed=False, howmany=None):
        """
        Method to find the best phot_std observation
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
                .limit(howmany)
                .all()
            )
