"""
This module is for helper classes for parsing file headers.  This will alleviate special case handling for
some data issues without needing to pollute the AstroData code or require a DRAGONS update.
"""
from abc import ABC
from datetime import datetime, date, timedelta
from typing import Any, Union, Callable, List

import dateutil
import numpy as np

from fits_storage.gemini_metadata_utils import gemini_processing_mode, gemini_telescope, gemini_instrument, \
    gemini_observation_type, gemini_observation_class, ratodeg, dectodeg, dmstodeg, gemini_readspeed_settings, \
    gemini_welldepth_settings, UT_DATETIME_SECS_EPOCH

__all__ = ["build_parser"]


REDUCTION_STATUS = {
    'FLAT': 'PROCESSED_FLAT',
    'BIAS': 'PROCESSED_BIAS',
    'FRINGE': 'PROCESSED_FRINGE',
    'DARK': 'PROCESSED_DARK',
    'ARC': 'PROCESSED_ARC',
    'SCIENCE': 'PROCESSED_SCIENCE',
    'STANDARD': 'PROCESSED_STANDARD',
    'SLITILLUM': 'PROCESSED_SLITILLUM',
    'BPM': 'PROCESSED_BPM',
    'PINHOLE': 'PROCESSED_PINHOLE'
}


class FileParser(ABC):
    """
    Abstract base for any file parser implementation.
    """
    def __init__(self, log=None):
        self._log = log

    def _try_or_none(self, fn: Callable[[], Any], message: str, require_in: List[Any] = None,
                     convert_fn: Callable[[Any], Any] = None) -> Any:
        """
        Helper wrapper to try accessing a field on an AstroData instance
        but gracefully handle failures as None values.

        This covers a variety of issues ultimately due to bad headers in the FITS
        files.  We would rather get the file ingested with what data can be
        parsed than fail completely.

        Parameters
        ----------
        fn : callable
            function to try
        message : str
            Message to log as a warning on failure

        Returns
        -------
        Value for the field, or None on error
        """
        try:
            if callable(fn):
                retval = fn()
            else:
                retval = fn
            if retval is not None and convert_fn is not None:
                retval = convert_fn(retval)
            if require_in and retval not in require_in:
                return None
            return retval
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as data_err:
            if self._log:
                self._log.warning("%s: %s" % (message, data_err))
            return None

    def adaptive_optics(self) -> bool:
        raise NotImplementedError()

    def airmass(self) -> Union[float, None]:
        raise NotImplementedError()

    def azimuth(self) -> Union[float, None]:
        raise NotImplementedError()

    def camera(self) -> str:
        raise NotImplementedError()

    def cass_rotator_pa(self):
        raise NotImplementedError()

    def central_wavelength(self) -> Union[float, None]:
        raise NotImplementedError()

    def coadds(self):
        raise NotImplementedError()

    def data_label(self) -> str:
        raise NotImplementedError()

    def dec(self) -> Union[float, None]:
        raise NotImplementedError()

    def detector_binning(self) -> str:
        raise NotImplementedError()

    def detector_roi_setting(self):
        raise NotImplementedError()

    def disperser(self) -> Union[str, None]:
        raise NotImplementedError()

    def elevation(self) -> Union[float, None]:
        raise NotImplementedError()

    def engineering(self) -> Union[bool, None]:
        raise NotImplementedError()

    def exposure_time(self) -> Union[float, None]:
        raise NotImplementedError()

    def filter_name(self) -> Union[str, None]:
        raise NotImplementedError()

    def focal_plane_mask(self) -> str:
        raise NotImplementedError()

    def gain_setting(self) -> str:
        raise NotImplementedError()

    def gcal_lamp(self) -> str:
        raise NotImplementedError()

    def instrument(self) -> str:
        raise NotImplementedError()

    def laser_guide_star(self) -> bool:
        raise NotImplementedError()

    def local_time(self):
        raise NotImplementedError()

    def mode(self) -> str:
        raise NotImplementedError()

    def object(self) -> str:
        raise NotImplementedError()

    def observation_class(self) -> str:
        raise NotImplementedError()

    def observation_id(self) -> str:
        raise NotImplementedError()

    def observation_type(self) -> str:
        raise NotImplementedError()

    def pre_image(self) -> bool:
        raise NotImplementedError()

    def processing(self) -> str:
        raise NotImplementedError()

    def processing_tag(self) -> str:
        raise NotImplementedError

    def program_id(self) -> str:
        raise NotImplementedError()

    def proprietary_coordinates(self) -> bool:
        raise NotImplementedError()

    def pupil_mask(self) -> str:
        raise NotImplementedError()

    def qa_state(self) -> str:
        raise NotImplementedError()

    def ra(self) -> Union[float, None]:
        raise NotImplementedError()

    def raw_bg(self) -> Union[float, None]:
        raise NotImplementedError()

    def raw_cc(self) -> Union[float, None]:
        raise NotImplementedError()

    def raw_iq(self) -> Union[float, None]:
        raise NotImplementedError()

    def raw_wv(self) -> Union[float, None]:
        raise NotImplementedError()

    def read_mode(self) -> Union[str, None]:
        raise NotImplementedError()

    def read_speed_setting(self):
        raise NotImplementedError()

    def reduction(self):
        raise NotImplementedError()

    def release(self) -> Union[date, None]:
        raise NotImplementedError()

    def requested_bg(self) -> Union[float, None]:
        raise NotImplementedError()

    def requested_cc(self) -> Union[float, None]:
        raise NotImplementedError()

    def requested_iq(self) -> Union[float, None]:
        raise NotImplementedError()

    def requested_wv(self) -> Union[float, None]:
        raise NotImplementedError()

    def site_monitoring(self) -> bool:
        raise NotImplementedError()

    def spectroscopy(self) -> bool:
        raise NotImplementedError()

    def telescope(self) -> str:
        raise NotImplementedError()

    def ut_datetime(self) -> Union[datetime, None]:
        raise NotImplementedError()

    def ut_datetime_secs(self) -> Union[int, None]:
        raise NotImplementedError()

    def wavefront_sensor(self):
        raise NotImplementedError()

    def wavelength_band(self):
        raise NotImplementedError()

    def well_depth_setting(self):
        raise NotImplementedError()


class AstroDataFileParser(FileParser):
    """
    FileParser implementation where we can use AstroData
    """
    def __init__(self, ad, log=None):
        super().__init__(log)
        self.ad = ad

    def adaptive_optics(self) -> bool:
        try:
            aofold = self.ad.phu.get('AOFOLD')
            return aofold == 'IN'
        except Exception:
            if self._log:
                self._log.warning("Unable to read AOFOLD header")
        return False

    def airmass(self) -> Union[float, None]:
        try:
            airmass = self.ad.airmass()
            airmass = float(airmass) if isinstance(airmass, str) else airmass
            if airmass is not None and (airmass > 50 or airmass < 1.0):
                if self.elevation() is not None:
                    try:
                        # approximate airmass with secant(90-elevation),
                        # converting to radians for numpy
                        sec_value = 1.0 / np.cos(np.radians(90-self.elevation()))
                        if self._log:
                            self._log.warning('Bad airmass value, using sec(90-elevation) as an estimate')
                        airmass = sec_value
                    except Exception as secant_exc:
                        if self._log:
                            self._log.warning('%s: %s' % ('estimate failed, using None for airmass', secant_exc))
                        airmass = None
                else:
                    # invalid airmass, just store None
                    # note also DB can't handle airmass >= 100 (10 cutoff above is per Paul Hirst
                    if self._log:
                        self._log.warning('Invalid airmass value and no elevation to estimate from, using None')
                    airmass = None
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as airmasserr:
            if self._log:
                self._log.warning("Unable to parse airmass: %s" % airmasserr)
            airmass = None
        return airmass

    def azimuth(self) -> Union[float, None]:
        azimuth = self._try_or_none(lambda: self.ad.azimuth(), "Unable to determine azimuth from datafile")
        if isinstance(azimuth, str):
            azimuth = dmstodeg(azimuth)
        return azimuth

    def camera(self) -> str:
        return self._try_or_none(lambda: self.ad.camera(pretty=True), "Unable to parse camera from header")

    def cass_rotator_pa(self):
        return self._try_or_none(lambda: self.ad.cass_rotator_pa(), "Unable to parse cass rotator pa")

    def central_wavelength(self):
        if 'SPECT' in self.ad.tags and 'GPI' not in self.ad.tags:
            dv= self._try_or_none(lambda: self.ad.central_wavelength(asMicrometers=True),
                                     "Unable to parse wavelength from header")
            if dv is not None:
                # The descriptor returns a numpy.float32 sometimes. SQLAlchemy
                # can't handle that.
                return float(dv)
        return None

    def coadds(self):
        return self._try_or_none(lambda: self.ad.coadds(), 'Unable to read co-adds from header')

    def data_label(self) -> str:
        data_label = self._try_or_none(lambda: self.ad.data_label(), 'Unable to parse datalabel from header',
                                       convert_fn=lambda x: str(x).upper())
        if data_label is None:
            data_label = ''
        return data_label

    def dec(self) -> Union[float, None]:
        if 'AZEL_TARGET' in self.ad.tags:
            return None
        dec = self._try_or_none(lambda: self.ad.dec(), 'Unable to parse DEC from header')
        if type(dec) is str:
            dec = dectodeg(dec)
        if dec is not None and (dec > 90.0 or dec < -90.0):
            dec = None
        return dec

    def detector_binning(self) -> str:
        dvx = self._try_or_none(lambda: self.ad.detector_x_bin(), "Unable to parse detector x bin from header")
        dvy = self._try_or_none(lambda: self.ad.detector_y_bin(), "Unable to parse detector y bin form header")
        if (dvx is not None) and (dvy is not None):
            return "%dx%d" % (dvx, dvy)
        return None

    def detector_roi_setting(self):
        return self._try_or_none(lambda: self.ad.detector_roi_setting(), "Unable to parse ROI setting from header")

    def disperser(self) -> Union[str, None]:
        # Need to remove invalid characters in disperser names, eg gnirs has
        # slashes
        disperser = self._try_or_none(lambda: self.ad.disperser(pretty=True), "Unable to read disperser information from datafile")
        if disperser is not None:
            return disperser.replace('/', '_')
        return None

    def elevation(self) -> Union[float, None]:
        elevation = self._try_or_none(lambda: self.ad.elevation(), "Unable to determine elevation from datafile")
        if isinstance(elevation, str):
            elevation = dmstodeg(elevation)
        return elevation

    def engineering(self) -> Union[bool, None]:
        engdata = self.ad.phu.get('ENG_DATA')
        if engdata is not None:
            try:
                engdata = bool(engdata)
            except Exception:
                engdata = None
        return engdata

    def exposure_time(self) -> Union[float, None]:
        exposure_time = self._try_or_none(lambda: self.ad.exposure_time(), "Unable to parse exposure time from header")

        # Protect the database from field overflow from junk.
        # The datatype is precision=8, scale=4
        if exposure_time is not None and (10000 > exposure_time >= 0):
            return exposure_time
        return None

    def filter_name(self) -> Union[str, None]:
        # Knock illegal characters out of filter names. eg NICI %s.
        # Spaces to underscores.
        try:
            filter_string = self._try_or_none(lambda: self.ad.filter_name(pretty=True),
                                              "Unable to get filter name from header")
            if filter_string:
                filter_string = filter_string.replace('%', '').replace(' ', '_')
            return filter_string
        except AttributeError:
            return None

    def focal_plane_mask(self) -> str:
        return self._try_or_none(lambda: self.ad.focal_plane_mask(pretty=True),
                                 "Unable to parse focal plane mask in header")

    def gain_setting(self) -> str:
        return self._try_or_none(lambda: self.ad.gain_setting(), "Unable to parse gain_setting from header",
                                 convert_fn=str)

    def gcal_lamp(self) -> str:
        return self._try_or_none(lambda: self.ad.gcal_lamp(), "Unable to parse gcal_lamp from header")

    def instrument(self) -> str:
        retval = self._try_or_none(lambda: self.ad.instrument(), 'Unable to read instrument from header',
                                   convert_fn=lambda x: gemini_instrument(x, other=True))
        return retval

    def laser_guide_star(self) -> bool:
        lgustage = self.ad.phu.get('LGUSTAGE')
        lgsloop = self.ad.phu.get('LGSLOOP')
        return (lgsloop == 'CLOSED') or (lgustage == 'IN')

    def local_time(self):
        return self._try_or_none(lambda: self.ad.local_time(), 'Unable to parse local time from header')

    def mode(self) -> str:
        mode = 'imaging'
        if 'SPECT' in self.ad.tags:
            mode = 'spectroscopy'
            if 'IFU' in self.ad.tags:
                mode = 'IFS'
            if 'MOS' in self.ad.tags:
                mode = 'MOS'
            if 'LS' in self.ad.tags:
                mode = 'LS'
        if 'GPI' in self.ad.tags and 'POL' in self.ad.tags:
            mode = 'IFP'
        return mode

    def object(self) -> str:
        return self._try_or_none(lambda: self.ad.object(), 'Unable to parse object from header')

    def observation_class(self) -> str:
        return self._try_or_none(lambda: self.ad.observation_class(), "Unable to determine observation class in datafile",
                                 convert_fn=gemini_observation_class)

    def observation_id(self) -> str:
        obsid = self._try_or_none(lambda: self.ad.observation_id(), 'Unable to parse Observation ID from header',
                                  convert_fn=lambda x: str(x).upper())
        return obsid

    def observation_type(self) -> str:
        observation_type = self._try_or_none(lambda: self.ad.observation_type(),
                                             "Unable to determine observation type in datafile",
                                             convert_fn=gemini_observation_type)
        if 'PINHOLE' in self.ad.tags:
            observation_type = 'PINHOLE'
        if 'RONCHI' in self.ad.tags:
            observation_type = 'RONCHI'

        return observation_type

    def pre_image(self) -> bool:
        try:
            pre_image = self.ad.phu.get("PREIMAGE")
            if pre_image is not None and (pre_image == "1" or pre_image == "T" or pre_image is True):
                return True
            else:
                return False
        except Exception:
            return False

    def processing(self) -> str:
        # If there is a valid processing review, that is definitive
        procrevw = gemini_processing_mode(self.ad.phu.get('PROCREVW'))
        if procrevw:
            return procrevw
        # Otherwise, processing intent is next up
        procitnt = gemini_processing_mode(self.ad.phu.get('PROCITNT'))
        if procitnt:
            return procitnt

        # OK, now we're into legacy stuff:
        procmode = self.ad.phu.get('PROCMODE')
        if procmode is None:
            procmode = self.ad.phu.get('PROCSCI')
        if procmode == 'ql':
            return 'Quick-Look'
        if procmode == 'sq':
            return 'Science-Quality'

        return 'Raw'

    def processing_tag(self):
        return self.ad.phu.get('PROCTAG')

    def program_id(self) -> str:
        return self._try_or_none(lambda: self.ad.program_id(), 'Unable to read Program ID from header',
                                 convert_fn=lambda x: str(x).upper())

    def proprietary_coordinates(self) -> bool:
        # Proprietary coordinates
        if self.ad.phu.get('PROP_MD') is True:
            return True
        return False

    def pupil_mask(self) -> str:
        return self._try_or_none(lambda: self.ad.pupil_mask(pretty=True), "Unable to parse pupil mask from header")

    def qa_state(self) -> str:
        # Set the derived QA state
        # MDF (Mask) files don't have QA state - set to Pass so they show up
        # as expected in search results
        if self.observation_type() == 'MASK':
            return 'Pass'
        else:
            qa_state = self._try_or_none(lambda: self.ad.qa_state(), 'Unable to parse qa_state')
            if qa_state not in ['Fail', 'CHECK', 'Undefined', 'Usable', 'Pass']:
                qa_state = 'Undefined'
            return qa_state

    def ra(self) -> Union[float, None]:
        if 'AZEL_TARGET' in self.ad.tags:
            return None
        ra = self._try_or_none(lambda: self.ad.ra(), 'Unable to parse RA from header')
        if type(ra) is str:
            ra = ratodeg(ra)
        if ra is not None and (ra > 360.0 or ra < 0.0):
            ra = None
        return ra

    def raw_bg(self) -> Union[float, None]:
        return self._try_or_none(lambda: self.ad.raw_bg(), "Unable to parse Raw BG from header")

    def raw_cc(self) -> Union[float, None]:
        return self._try_or_none(lambda: self.ad.raw_cc(), "Unable to parse Raw CC from header")

    def raw_iq(self) -> Union[float, None]:
        return self._try_or_none(lambda: self.ad.raw_iq(), "Unable to parse Raw IQ from header")

    def raw_wv(self) -> Union[float, None]:
        return self._try_or_none(lambda: self.ad.raw_wv(), "Unable to parse Raw WV from header")

    def read_mode(self) -> Union[str, None]:
        return self._try_or_none(lambda: self.ad.read_mode(), "Unable to parse read_mode from header",
                                 convert_fn=lambda x: str(x).replace(' ', '_'))

    def read_speed_setting(self):
        return self._try_or_none(lambda: self.ad.read_speed_setting(), "Unable to parse read speed from header",
                                 require_in=gemini_readspeed_settings)

    def reduction(self):
        # Set the reduction state
        # Note - these are in order - a processed_flat will have
        # both PREPARED and PROCESSED_FLAT in it's types.
        # Here, ensure "highest" value wins.
        tags = self.ad.tags
        if 'PROCESSED_SCIENCE' in tags:
            return 'PROCESSED_SCIENCE'
        elif 'PROCESSED' in tags:
            # Use the image type tag (BIAS, FLAT, ...) to obtain the
            # appropriate reduction status from the lookup table
            kind = list(tags.intersection(list(REDUCTION_STATUS.keys())))
            try:
                return REDUCTION_STATUS[kind[0]]
            except (KeyError, IndexError):
                # Supposedly a processed file, but not any that we know of!
                # Mark it as prepared, just in case
                # TODO: Maybe we want to signal an error here?
                return 'PROCESSED_UNKNOWN'
        elif 'PREPARED' in tags:
            return 'PREPARED'
        else:
            return 'RAW'

    def release(self) -> Union[date, None]:
        try:
            reldatestring = self.ad.phu.get('RELEASE')
            if reldatestring:
                reldts = "%s 00:00:00" % reldatestring
                return dateutil.parser.parse(reldts).date()
            return None
        except Exception:
            # This exception will trigger if RELEASE date is missing or malformed.
            return None

    def requested_bg(self) -> Union[float, None]:
        return self._try_or_none(lambda: self.ad.requested_bg(), "Unable to parse requested BG from header")

    def requested_cc(self) -> Union[float, None]:
        return self._try_or_none(lambda: self.ad.requested_cc(), "Unable to parse requested CC from header")

    def requested_iq(self) -> Union[float, None]:
        return self._try_or_none(lambda: self.ad.requested_iq(), "Unable to parse requested IQ from header")

    def requested_wv(self) -> Union[float, None]:
        return self._try_or_none(lambda: self.ad.requested_wv(), "Unable to parse requested WV from header")

    def site_monitoring(self) -> bool:
        """
        Check if this file is a site monitoring file.

        For now, this is only true if the instrument is set to `GS_ALLSKYCAMERA`.

        Returns
        -------
        bool
            Returns `True` when instrument `GS_ALLSKYCAMERA`
        """
        instr = self._try_or_none(lambda: self.ad.instrument(), 'Unable to parse instrument from header')
        if instr == 'GS_ALLSKYCAMERA':
            return True
        else:
            return False

    def spectroscopy(self) -> bool:

        # Hack for GHOST data... GHOST bundles don't have the SPECT tag as they
        # also contain the slit viewer images. But any sane archive user wants
        # GHOST bundles to show up if you search for spectroscopy...
        if 'GHOST' in self.ad.tags and 'BUNDLE' in self.ad.tags:
            return True
        return 'SPECT' in self.ad.tags

    def telescope(self) -> str:
        return gemini_telescope(self.ad.telescope())

    def ut_datetime(self) -> Union[datetime, None]:
        return self._try_or_none(lambda: self.ad.ut_datetime(), 'Unable to parse UT datetime from header')

    def ut_datetime_secs(self) -> Union[int, None]:
        ut_datetime = self.ut_datetime()
        if ut_datetime is not None:
            delta = ut_datetime - UT_DATETIME_SECS_EPOCH
            return int(delta.total_seconds())
        else:
            # ut_datetime is None, make ut_datetime_secs None too.
            return None

    def wavefront_sensor(self):
        return self._try_or_none(lambda: self.ad.wavefront_sensor(), "Unable to read wavefront sensor from header")

    def wavelength_band(self):
        return self._try_or_none(lambda: self.ad.wavelength_band(), "Unable to read wavelength band from header")

    def well_depth_setting(self):
        return self._try_or_none(lambda: self.ad.well_depth_setting(), "Unable to parse well depth setting from header",
                                 require_in=gemini_welldepth_settings)


class AlopekeZorroFileParser(AstroDataFileParser):
    def __init__(self, ad, default_telescope):
        super().__init__(ad)
        self._default_telescope = default_telescope

    def _extract_wcs(self):
        ctype1 = self.ad.phu.get('CTYPE1')
        ctype2 = self.ad.phu.get('CTYPE2')
        crval1 = self.ad.phu.get('CRVAL1')
        crval2 = self.ad.phu.get('CRVAL2')
        return ctype1, ctype2, crval1, crval2

    def ra(self) -> Union[float, None]:
        ra = None
        if 'AZEL_TARGET' in self.ad.tags:
            return None
        try:
            ra = self.ad.wcs_ra()
        except Exception:
            ctype1, ctype2, crval1, crval2 = self._extract_wcs()
            if ctype1 == 'RA---TAN' or ctype1 == 'RA--TAN':  # Zorro sometimes is broken with RA--TAN
                ra = crval1
            if ctype2 == 'RA---TAN' or ctype2 == 'RA--TAN':  # Zorro sometimes is broken with RA--TAN
                ra = crval2
        if ra is None:
            try:
                ra = self.ad.ra()
            except Exception:
                if self._log and hasattr(self.ad, "filename"):
                    self._log.warning(f"Final ra fallback, unable to determine ra for file {self.ad.filename}")
        if type(ra) is str:
            ra = ratodeg(ra)
        if ra is not None and (ra > 360.0 or ra < 0.0):
            ra = None
        return ra

    def dec(self) -> Union[float, None]:
        if 'AZEL_TARGET' in self.ad.tags:
            return None
        dec = None
        try:
            dec = self.ad.wcs_dec()
        except Exception:
            ctype1, ctype2, crval1, crval2 = self._extract_wcs()
            if ctype1 == 'DEC--TAN':
                dec = crval1
            if ctype2 == 'DEC--TAN':
                dec = crval2
        if dec is None:
            try:
                dec = self.ad.dec()
            except Exception:
                if self._log and hasattr(self.ad, "filename"):
                    self._log.warning(f"Final dec fallback, unable to determine dec for file {self.ad.filename}")
        if type(dec) is str:
            dec = dectodeg(dec)
        if dec is not None and (dec > 90.0 or dec < -90.0):
            dec = None
        return dec

    def exposure_time(self) -> Union[float, None]:
        exposure_time = super().exposure_time()
        if isinstance(exposure_time, str):
            return float(exposure_time)
        return exposure_time

    def object(self) -> str:
        object = super().object()
        if object:
            return object
        if 'Object' in self.ad.phu:
            return self.ad.phu['Object']

    def observation_class(self) -> str:
        observation_class = super().observation_class()
        if observation_class:
            return observation_class
        return 'science'

    def observation_id(self) -> str:
        prgid = super().program_id()
        obsid = super().observation_id()
        if obsid and obsid != prgid:
            return obsid
        if prgid:
            return f'{prgid}-0'
        return None

    def observation_type(self) -> str:
        observation_type = super().observation_type()
        if observation_type:
            if isinstance(observation_type, str):
                return observation_type.upper()
            return observation_type
        return 'OBJECT'


    def telescope(self) -> str:
        telescope = super().telescope()
        if telescope:
            return telescope
        return self._default_telescope

    def data_label(self) -> str:
        data_label = super().data_label()
        if data_label:
            return data_label
        obsid = self.observation_id()
        if obsid:
            return f'{obsid}-0'
        return None


class IGRINSFileParser(AstroDataFileParser):
    def program_id(self) -> str:
        program_id = super().program_id()
        if program_id is None:
            try:
                program_id = self.ad.phu['GEMPRID']
            except (TypeError, AttributeError, KeyError, ValueError, IndexError) as data_err:
                program_id = None
        return program_id

    def observation_id(self) -> str:
        progid = self.program_id()
        try:
            obsid = self.ad.phu['OBSID']
        except (TypeError, AttributeError, KeyError, ValueError, IndexError) as data_err:
            obsid = None
        if obsid is None and progid is None:
            return None
        if (obsid is None and progid is not None) or obsid == progid:
            return f'{progid}-0'
        if obsid is not None and progid is None:
            return obsid
        if isinstance(obsid, int):
            return f'{progid}-{obsid}'
        return obsid

    def data_label(self) -> str:
        data_label = super().data_label()
        if data_label:
            return data_label
        else:
            return '%s-0' % self.observation_id()

    def release(self) -> Union[date, None]:
        release = super().release()
        if release:
            return release
        else:
            # fix RELEASE header if missing, we base this on DATE-OBS + 1 year
            if 'RELEASE' not in self.ad.phu and 'DATE-OBS' in self.ad.phu and self.ad.phu['DATE-OBS'] is not None:
                try:
                    dateobs = self.ad.phu['DATE-OBS']
                    if len(dateobs) >= 10:
                        dateobs = dateobs[0:10]
                        dt = datetime.strptime(dateobs, '%Y-%m-%d')
                        return (dt + timedelta(days=365)).date()
                except Exception as e:
                    return None

    def telescope(self) -> str:
        """
        IGRINS specifies the wrong telescope value with a ' ' in place of a '-'.

        :return:  str name of telescope
        """
        retval = gemini_telescope(self.ad.telescope())
        if retval is None and self.ad.telescope() is not None and ' ' in self.ad.telescope():
            return gemini_telescope(self.ad.telescope().replace(' ', '-'))

class IGRINS2FileParser(AstroDataFileParser):
    def exposure_time(self):
        # Two exposure times, in the first and second extensions
        try:
            a = self.ad[0].hdr.get('EXPTIME')
        except Exception:
            a = None
        try:
            b = self.ad[1].hdr.get('EXPTIME')
        except Exception:
            b = None
        if a is None and b is None:
            return None
        if a is None:
            return b
        if b is None:
            return a
        try:
            return max(a, b)
        except Exception:
            return None

class NICIFileParser(AstroDataFileParser):
    def exposure_time(self) -> Union[float, None]:
        # NICI exposure times are a pain, because there's two of them...
        # Except they're always the same.
        try:
            # NICI exposure_time descriptor is broken
            et_b = self.ad.phu.get('ITIME_B')
            et_r = self.ad.phu.get('ITIME_R')
            exposure_time = et_b if et_b else et_r

            # Protect the database from field overflow from junk.
            # The datatype is precision=8, scale=4
            if exposure_time is not None and (10000 > exposure_time >= 0):
                return exposure_time
        except (TypeError, AttributeError, KeyError, ValueError, IndexError):
            if self._log:
                self._log.warning("Type error parsing exposure time, ignoring")
        return None


class GMOSFileParser(AstroDataFileParser):
    def read_mode(self) -> Union[str, None]:
        return "NodAndShuffle" \
            if 'NODANDSHUFFLE' in self.ad.tags else "Classic"


class GHOSTFileParser(AstroDataFileParser):
    def dedictify(self, value, sum=False, min=False, scionly=False):
        arm = self.ad.arm()
        if isinstance(value, dict):
            if arm is not None:
                return value.get(arm, None)
            else:
                if sum:
                    retval = 0.0
                    for k, v in value.items():
                        if v is not None:
                            if isinstance(v, str):
                                try:
                                    v = float(v)
                                except:
                                    v = 0.0
                            retval += v
                    return retval
                if min:
                    retval = None
                    for k, v in value.items():
                        if retval is None or (v is not None and v<retval):
                            retval = v
                    return retval
                if scionly:
                    retval = None
                    bval = value.get('blue')
                    rval = value.get('red')
                    if bval == rval:
                        return bval
                    else:
                        return None
        else:
            return value

    def exposure_time(self) -> Union[float, None]:
        et = self.ad.exposure_time()
        if isinstance(et, dict):
            # There are a few files that trip this KeyError. Could handle it
            # better if there's onely one camera present...
            try:
                val = min(self.ad.number_of_exposures()[camera] * self.ad.exposure_time()[camera] for camera in ('blue', 'red'))
            except KeyError:
                val = None
        else:
            val =  super().exposure_time()
        if isinstance(val, str):
            try:
                val = float(str)
            except:
                val = None
        return val

    def detector_binning(self) -> str:
        dvx = self.dedictify(self.ad.detector_x_bin(), scionly=True)
        dvy = self.dedictify(self.ad.detector_y_bin(), scionly=True)
        if (dvx is not None) and (dvy is not None):
            return "%dx%d" % (dvx, dvy)
        return None

    def gain_setting(self):
        gs = super().gain_setting()
        if gs is None or isinstance(gs, dict) or gs.startswith('{'):
            return None
        return gs

    def read_speed_setting(self):
        # GHOST is an oddball here. If this is a raw data bundle (ie we got
        # a dict, then we construct some custom red:foo,blue:bar strings that
        # are also entries in the read_speed_setting enum, which I'm not keen
        # on, but it's the only pragmatic solution for having these searchable.
        rss = self.ad.read_speed_setting()
        if isinstance(rss, dict):
            if 'blue' in rss.keys() and 'red' in rss.keys():
                return f"red:{rss['red']},blue:{rss['blue']}"
            else:
                # we got a dict but without red and blue.
                return None
        # What we got wasn't a dict. Suck it and see
        return rss

class GRACESFileParser(AstroDataFileParser):
    def reduction(self) -> str:
        reduction = 'RAW'
        try:
            if self.ad.phu.get('REDUCTIO') is not None:
                reduction = 'PROCESSED_SCIENCE'
        except:
            pass
        return reduction

    def processing(self) -> str:
        procmode = 'Raw'
        try:
            if self.ad.phu.get('REDUCTIO') is not None:
                procmode = 'Quick-Look'
        except:
            pass
        return procmode


def build_parser(ad, log) -> FileParser:
    if hasattr(ad, 'tags') and 'GMOS' in ad.tags:
        return GMOSFileParser(ad, log)
    try:
        if ad.instrument() is not None:
            if ad.instrument().upper() == 'GHOST':
                return GHOSTFileParser(ad, log)
            if ad.instrument().upper() == 'ALOPEKE':
                return AlopekeZorroFileParser(ad, default_telescope='Gemini-North')
            elif ad.instrument().upper() == 'ZORRO':
                return AlopekeZorroFileParser(ad, default_telescope='Gemini-South')
            elif ad.instrument().upper() == 'NICI':
                return NICIFileParser(ad, log)
            elif ad.instrument().upper() == 'IGRINS':
                return IGRINSFileParser(ad, log)
            elif ad.instrument().upper() == 'GRACES':
                return GRACESFileParser(ad, log)
            elif ad.instrument().upper() == 'IGRINS-2':
                return IGRINS2FileParser(ad, log)
    except:
        pass
    return AstroDataFileParser(ad, log)
