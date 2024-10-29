sayselection_defs = {
    'program_id': 'Program ID',
    'observation_id': 'Observation ID',
    'data_label': 'Data Label',
    'date': 'Date',
    'daterange': 'Daterange',
    'inst': 'Instrument',
    'observation_type': 'ObsType',
    'observation_class': 'ObsClass',
    'filename': 'Filename',
    'processing': 'Processing',
    'object': 'Object Name',
    'engineering': 'Engineering Data',
    'science_verification': 'Science Verification Data',
    'calprog': 'Calibration Program',
    'disperser': 'Disperser',
    'focal_plane_mask': 'Focal Plane Mask',
    'pupil_mask': 'Pupil Mask',
    'binning': 'Binning',
    'caltype': 'Calibration Type',
    'caloption': 'Calibration Option',
    'photstandard': 'Photometric Standard',
    'reduction': 'Reduction State',
    'twilight': 'Twilight',
    'az': 'Azimuth',
    'el': 'Elevation',
    'ra': 'RA',
    'dec': 'Dec',
    'sr': 'Search Radius',
    'crpa': 'CRPA',
    'telescope': 'Telescope',
    'detector_roi': 'Detector ROI',
    'detector_gain_setting': 'Gain',
    'detector_readspeed_setting': 'Read Speed',
    'detector_welldepth_setting': 'Well Depth',
    'detector_readmode_setting': 'Read Mode',
    'filepre': 'File Prefix',
    'mode': 'Spectroscopy Mode',
    'cenwlen': 'Central Wavelength',
    'camera': 'Camera',
    'exposure_time': 'Exposure Time',
    'coadds': 'Coadds',
    'mdready': 'MetaData OK',
    'gpi_astrometric_standard': 'GPI Astrometric Standard',
    'night': 'Observing Night',
    'nightrange': 'Observing Night Range',
    }


def say(self):
    """
    Returns a string that describes the selection dictionary passed in suitable
    for pasting into html.

    """
    # First we're going to try to collect various parts of the selection in
    # a list that we can join later.

    # Collect simple associations of the 'key: value' type from the
    # sayselection_defs dictionary
    parts = ["%s: %s" % (sayselection_defs[key], self._seldict[key])
             for key in sayselection_defs if key in self._seldict]
    
    if self._seldict.get('site_monitoring'):
        parts.append('Is Site Monitoring Data')

    # More complicated selections from here on
    if 'spectroscopy' in self._seldict:
        parts.append('Spectroscopy' if self._seldict['spectroscopy'] else 'Imaging')

    if 'qa_state' in self._seldict:
        qa_state_dict = {
            'Win': "Win (Pass or Usable)",
            'NotFail': "Not Fail",
            'Lucky': "Lucky (Pass or Undefined)"
            }

        sel = self._seldict['qa_state']
        parts.append('QA State: ' + qa_state_dict.get(sel, sel))

    if 'ao' in self._seldict:
        parts.append("Adaptive Optics in beam"
                     if self._seldict['ao'] == 'AO'
                     else "No Adaptive Optics in beam")

    if 'lgs' in self._seldict:
        parts.append("LGS" if self._seldict['lgs'] == 'LGS' else "NGS")

    # If any of the previous tests contributed parts to the list, this will
    # create a return string like '; ...; ...; ...'. Otherwise we get an
    # empty string.
    ret = '; '.join([''] + parts)

    if 'notrecognised' in self._seldict:
        return ret + ". WARNING: I didn't understand these (case-sensitive) " \
                     "words: %s" % self._seldict['notrecognised']

    return ret
