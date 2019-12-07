"""
This module holds the CalibrationGHOST class
"""
import datetime

from ..orm.diskfile import DiskFile
from ..orm.header import Header
from ..orm.ghost import Ghost
from .calibration import Calibration, not_processed, not_imaging, not_spectroscopy

from sqlalchemy.orm import join

import math

class CalibrationGHOST(Calibration):
    """
    This class implements a calibration manager for GHOST.
    It is a subclass of Calibration
    """
    instrClass = Ghost
    instrDescriptors = (
        'disperser',
        'filter_name',
        'focal_plane_mask',
        'detector_x_bin',
        'detector_y_bin',
        'amp_read_area',
        'read_speed_setting',
        'gain_setting',
        'res_mode',
        'prepared',
        'overscan_trimmed',
        'overscan_subtracted'
        'want_before_arc'
        )

    def __init__(self, session, *args, **kwargs):
        # Need to super the parent class __init__ to get want_before_arc
        # keyword in
        super(CalibrationGHOST, self).__init__(session, *args, **kwargs)

        if self.descriptors is None and self.instrClass is not None:
            self.descriptors['want_before_arc'] = self.header.want_before_arc
            iC = self.instrClass
            query = session.query(iC).filter(
                iC.header_id == self.descriptors['header_id'])
            inst = query.first()

            # Populate the descriptors dictionary for the instrument
            for descr in self.instrDescriptors:
                self.descriptors[descr] = getattr(inst, descr, None)

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

            # If it (is spectroscopy) and
            # (is an OBJECT) and
            # (is not a Twilight) and
            # (is not a specphot)
            # then it needs an arc, flat, spectwilight, specphot
            if ((self.descriptors['spectroscopy'] == True) and
                    (self.descriptors['observation_type'] == 'OBJECT') and
                    (self.descriptors['object'] != 'Twilight') and
                    (self.descriptors['observation_class'] not in ['partnerCal', 'progCal'])):
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

            # If it is MOS then it needs a MASK
            if 'MOS' in self.types:
                self.applicable.append('mask')

    @not_imaging
    def arc(self, processed=False, howmany=2):
        """
        This method identifies the best GHOST ARC to use for the target
        dataset.
        """
        ab = self.descriptors.get('want_before_arc', None)
        # Default 2 arcs, hopefully one before and one after
        if ab is not None:
            howmany = 1
        else:
            howmany = howmany if howmany else 2
        filters = []
        # # Must match focal_plane_mask only if it's not the 5.0arcsec slit in the target, otherwise any longslit is OK
        # if self.descriptors['focal_plane_mask'] != '5.0arcsec':
        #     filters.append(Ghost.focal_plane_mask == self.descriptors['focal_plane_mask'])
        # else:
        #     filters.append(Ghost.focal_plane_mask.like('%arcsec'))

        if ab:
            # Add the 'before' filter
            filters.append(Header.ut_datetime < self.descriptors['ut_datetime'])
        elif ab is None:
            # No action required
            pass
        else:
            # Add the after filter
            filters.append(Header.ut_datetime > self.descriptors['ut_datetime'])

        # # The science amp_read_area must be equal or substring of the cal amp_read_area
        # # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # # have a subset of the amps thus we must do the substring match
        # if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
        #     filters.append(Ghost.amp_read_area == self.descriptors['amp_read_area'])
        # elif self.descriptors['amp_read_area'] is not None:
        #         filters.append(Ghost.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                .arc(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Ghost.disperser,
                                   Ghost.filter_name)
                .tolerance(central_wavelength=0.001)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
            )

    def dark(self, processed=False, howmany=None):
        """
        Method to find best GHOST Dark frame for the target dataset.
        """
        if howmany is None:
            howmany = 1 if processed else 15

        filters = []
        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Ghost.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Ghost.amp_read_area.contains(self.descriptors['amp_read_area']))

        # Must match exposure time.


        return (
            self.get_query()
                .dark(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Ghost.read_speed_setting,
                                   Ghost.gain_setting)
                .tolerance(exposure_time = 50.0)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
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
            filters.append(Ghost.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
            filters.append(Ghost.amp_read_area.contains(self.descriptors['amp_read_area']))

        # The Overscan section handling: this only applies to processed biases
        # as raw biases will never be overscan trimmed or subtracted, and if they're
        # processing their own biases, they ought to know what they want to do.
        if processed:
            if self.descriptors['prepared'] == True:
                # If the target frame is prepared, then we match the overscan state.
                filters.append(Ghost.overscan_trimmed == self.descriptors['overscan_trimmed'])
                filters.append(Ghost.overscan_subtracted == self.descriptors['overscan_subtracted'])
            else:
                # If the target frame is not prepared, then we don't know what thier procesing intentions are.
                # we could go with the default (which is trimmed and subtracted).
                # But actually it's better to just send them what we have, as we has a mishmash of both historically
                #filters.append(Ghost.overscan_trimmed == True)
                #filters.append(Ghost.overscan_subtracted == True)
                pass

        return (
            self.get_query()
                .bias(processed)
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                  Ghost.detector_x_bin,
                                  Ghost.detector_y_bin,
                                  Ghost.read_speed_setting,
                                  Ghost.gain_setting)
                # Absolute time separation must be within 3 months
                .max_interval(days=90)
                .all(howmany)
            )

    def imaging_flat(self, processed, howmany, flat_descr, filt, sf=False):
        if howmany is None:
            howmany = 1 if processed else 20

        if processed:
            query = self.get_query().PROCESSED_FLAT()
        elif sf:
            # Find the relevant slit flat
            query = self.get_query().spectroscopy(
                False).observation_type('FLAT')
        else:
            # Imaging flats are twilight flats
            # Twilight flats are dayCal OBJECT frames with target Twilight
            query = self.get_query().raw().dayCal().OBJECT().object('Twilight')
        return (
            query.add_filters(*filt)
                 .match_descriptors(*flat_descr)
                 # Absolute time separation must be within 6 months
                 .max_interval(days=180)
                 .all(howmany)
            )

    def spectroscopy_flat(self, processed, howmany, flat_descr, filt):
        if howmany is None:
            howmany = 1 if processed else 2

        ifu = mos_or_ls = False
        el_thres = 0.0
        under_85 = False
        crpa_thres = 0.0
        # QAP might not give us these for now. Remove this 'if' later when it does
        if self.descriptors.get('elevation') is not None:
            # Spectroscopy flats also have to somewhat match telescope position for flexure, as follows
            # this is from FitsStorage TRAC #43 discussion with KR 20130425. This code defines the
            # thresholds and the conditions where they apply
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
            except AttributeError:
                # Just in case disperser is None
                pass
            under_85 = self.descriptors['elevation'] < 85

            # crpa*cos(el) must be within el_thres degrees, ie crpa must be within el_thres / cos(el)
            # when el=90, cos(el) = 0 and the range is infinite. Only check at lower elevations
            if under_85:
                crpa_thres = el_thres / math.cos(math.radians(self.descriptors['elevation']))

            # unset condition flags if thresholds are zero (bug in calibration.py necessitates this)
            if crpa_thres == 0: under_85 = False
            if el_thres == 0: ifu = mos_or_ls = False

        return (
            self.get_query()
                .flat(processed)
                .add_filters(*filt)
                .match_descriptors(*flat_descr)
            # Central wavelength is in microns (by definition in the DB table).
                .tolerance(central_wavelength=0.001)

            # Spectroscopy flats also have to somewhat match telescope position for flexure, as follows
            # this is from FitsStorage TRAC #43 discussion with KR 20130425
            # See the comments above to explain the thresholds
                .tolerance(condition = ifu, elevation=el_thres)
                .tolerance(condition = mos_or_ls, elevation=el_thres)
                .tolerance(condition = under_85, cass_rotator_pa=crpa_thres)

            # Absolute time separation must be within 6 months
                .max_interval(days=180)
                .all(howmany)
            )

    def flat(self, processed=False, howmany=None):
        """
        Method to find the best GHOST FLAT fields for the target dataset
        """

        filters = []

        # Common descriptors for both types of flat
        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        flat_descriptors = (
            Header.instrument,
            Ghost.filter_name,
            Ghost.read_speed_setting,
            Ghost.gain_setting,
            Ghost.res_mode,
            Header.spectroscopy,
            # Focal plane mask must match for imaging too... To avoid daytime thru-MOS mask imaging "flats"
            Ghost.focal_plane_mask,
            Ghost.disperser, # this can be common-mode as imaging is always 'MIRROR'
            )

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            flat_descriptors = flat_descriptors + (Ghost.amp_read_area,)
        elif self.descriptors['amp_read_area'] is not None:
            filters.append(Ghost.amp_read_area.contains(self.descriptors['amp_read_area']))

        if self.descriptors['spectroscopy']:
            return self.spectroscopy_flat(processed, howmany, flat_descriptors, filters)
        else:
            return self.imaging_flat(processed, howmany, flat_descriptors, filters)

    def processed_slitflat(self, howmany=None):
        """
        Method to find the best GHOST SLITFLAT for the target dataset
        """
        if 'SLITV' in self.types:
            return self.flat(True, howmany)

        filters = []
        filters.append(Header.spectroscopy == False)

        # Common descriptors for both types of flat
        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        flat_descriptors = (
            Header.instrument,
            Ghost.filter_name,
            Ghost.read_speed_setting,
            Ghost.gain_setting,
            Ghost.res_mode,
            Ghost.disperser, # this can be common-mode as imaging is always 'MIRROR'
            )

        return self.imaging_flat(False, howmany, flat_descriptors, filters,
                                 sf=True)

    def processed_slit(self, howmany=None):
        descripts = (
            Header.instrument,
            Header.observation_type,
            Ghost.res_mode,
            )

        filters = (
            Ghost.detector_name.startswith('Sony-ICX674'),
        )

        return (
            self.get_query()
                .reduction(  # this may change pending feedback from Kathleen
                    'PROCESSED_ARC' if
                    self.descriptors['observation_type'] == 'ARC' else
                    'PREPARED'
                )
                .spectroscopy(False)
                .match_descriptors(*descripts)
                .add_filters(*filters)
                # Need to use the slit image that matches the input observation;
                # needs to match within 30 seconds!
                .max_interval(seconds=30)
                .all(howmany)
            )

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
            filters.append(Ghost.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Ghost.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                .PROCESSED_FRINGE()
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Ghost.detector_x_bin,
                                   Ghost.detector_y_bin,
                                   Ghost.filter_name)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
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
            filters.append(Ghost.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Ghost.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                # They are OBJECT spectroscopy frames with target twilight
                .raw().OBJECT().spectroscopy(True).object('Twilight')
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Ghost.detector_x_bin,
                                   Ghost.detector_y_bin,
                                   Ghost.filter_name,
                                   Ghost.disperser,
                                   Ghost.focal_plane_mask)
                # Must match central wavelength to within some tolerance.
                # We don't do separate ones for dithers in wavelength?
                # tolerance = 0.02 microns
                .tolerance(central_wavelength=0.02)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
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
            filters.append(Ghost.focal_plane_mask.contains('arcsec'))
            tol = 0.10 # microns
        else:
            filters.append(Ghost.focal_plane_mask == self.descriptors['focal_plane_mask'])
            tol = 0.05 # microns

        # The science amp_read_area must be equal or substring of the cal amp_read_area
        # If the science frame uses all the amps, then they must be a direct match as all amps must be there
        # - this is more efficient for the DB as it will use the index. Otherwise, the science frame could
        # have a subset of the amps thus we must do the substring match
        if self.descriptors['detector_roi_setting'] in ['Full Frame', 'Central Spectrum']:
            filters.append(Ghost.amp_read_area == self.descriptors['amp_read_area'])
        elif self.descriptors['amp_read_area'] is not None:
                filters.append(Ghost.amp_read_area.contains(self.descriptors['amp_read_area']))

        return (
            self.get_query()
                # They are OBJECT partnerCal or progCal spectroscopy frames with target not twilight
                .raw().OBJECT().spectroscopy(True)
                .add_filters(Header.observation_class.in_(['partnerCal', 'progCal']),
                             Header.object != 'Twilight',
                             *filters)
                # Found lots of examples where detector binning does not match, so we're not adding those
                .match_descriptors(Header.instrument,
                                   Ghost.filter_name,
                                   Ghost.disperser)
                # Must match central wavelength to within some tolerance.
                # We don't do separate ones for dithers in wavelength?
                .tolerance(central_wavelength=tol)
                # Absolute time separation must be within 1 year
                .max_interval(days=365)
                .all(howmany)
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
                                   Ghost.filter_name)
                # Absolute time separation must be within 1 days
                .max_interval(days=1)
                .all(howmany)
            )

    # We don't handle processed ones (yet)
    @not_processed
    def mask(self, processed=False, howmany=None):
        """
        Method to find the MASK (MDF) file
        """
        # Default number to associate
        howmany = howmany if howmany else 1

        return (
            self.get_query()
                # They are MASK observation type
                # The focal_plane_mask of the science file must match the data_label of the MASK file (yes, really...)
                # Cant force an instrument match as sometimes it just says GHOST in the mask...
                .add_filters(Header.observation_type == 'MASK',
                             Header.data_label == self.descriptors['focal_plane_mask'],
                             Header.instrument.startswith('GHOST'))
                .all(howmany)
            )
