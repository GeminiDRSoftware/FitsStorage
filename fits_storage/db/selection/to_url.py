import urllib.parse
import urllib.error

import fits_storage.gemini_metadata_utils as gmu


def to_url(self, with_columns=False):
    """
    Receives a selection dictionary, parses values and converts to URL string
    """
    self._url = ''

    # We only want one of data_label, observation_id, program_id in the URL,
    # the most specific one should carry.
    if 'data_label' in self._seldict:
        self._seldict.pop('observation_id', None)
        self._seldict.pop('program_id', None)
    if 'observation_id' in self._seldict:
        self._seldict.pop('program_id', None)

    for key in self._seldict:
        if key in {'warning', 'Search', 'ObsLogsOnly'}:
            # Don't put the warning text or search buttons in the URL
            pass
        elif key == 'data_label':
            # See if it is a valid data_label
            dl = gmu.GeminiDataLabel(self._seldict[key])
            if dl.valid:
                # Regular form, just stuff it in
                self._url += '/%s' % self._seldict[key]
            else:
                # It's a non-standard one
                self._url += '/datalabel=%s' % self._seldict[key]
        elif key == 'observation_id':
            # See if it is a valid observation id, or if we need to add obsid=
            go = gmu.GeminiObservation(self._seldict[key])
            if go.valid:
                # Regular obs id, just stuff it in
                self._url += '/%s' % self._seldict[key]
            else:
                # It's a non-standard one
                self._url += '/obsid=%s' % self._seldict[key]
        elif key == 'program_id':
            # See if it is a valid program id, or if we need to add progid=
            gp = gmu.GeminiProgram(self._seldict[key])
            if gp.valid:
                # Regular program id, just stuff it in
                self._url += '/%s' % self._seldict[key]
            else:
                # It's a non-standard one
                self._url += '/progid=%s' % self._seldict[key]
        elif key == 'object':
            # We need to double-escape this because the webserver/wsgi code (
            # outside our control) will de-escape it for us and we'll be left
            # with, for instance, /s that we can't differentiate from those
            # in the path.
            self._url += '/object=%s' % urllib.parse.quote(
                self._seldict[key]).replace('/', '%252F')
        elif key == 'publication':
            self._url += '/publication=%s' % urllib.parse.quote(self._seldict[key])
        elif key == 'spectroscopy':
            if self._seldict[key] is True:
                self._url += '/spectroscopy'
            else:
                self._url += '/imaging'
        elif key in {'ra', 'dec', 'sr', 'filter', 'cenwlen', 'disperser',
                     'camera', 'exposure_time', 'coadds', 'pupil_mask',
                     'PIname', 'ProgramText', 'gain', 'readspeed', 'welldepth',
                     'readmode', 'date', 'daterange', 'night', 'nightrange'}:
            self._url += '/%s=%s' % (key, self._seldict[key])
        elif key == 'cols':
            if with_columns:
                self._url += '/cols=%s' % self._seldict['cols']
        elif key == 'present':
            if self._seldict[key] is True:
                self._url += '/present'
            else:
                self._url += '/notpresent'
        elif key == 'canonical':
            if self._seldict[key] is True:
                self._url += '/canonical'
            else:
                self._url += '/notcanonical'
        elif key == 'twilight':
            if self._seldict[key] is True:
                self._url += '/twilight'
            else:
                self._url += '/nottwilight'
        elif key == 'engineering':
            if self._seldict[key] is True:
                self._url += '/engineering'
            elif self._seldict[key] is False:
                self._url += '/notengineering'
            else:
                self._url += '/includeengineering'
        elif key == 'calprog':
            if self._seldict[key] is True:
                self._url += '/calprog'
            elif self._seldict[key] is False:
                self._url += '/notcalprog'
        elif key == 'science_verification':
            if self._seldict[key] is True:
                self._url += '/science_verification'
            else:
                self._url += '/notscience_verification'
        elif key == 'detector_roi':
            if self._seldict[key] == 'Full Frame':
                self._url += '/fullframe'
            elif self._seldict[key] == 'Central Spectrum':
                self._url += '/centralspectrum'
            elif self._seldict[key] == 'Central Stamp':
                self._url += '/centralstamp'
            elif self._seldict[key] == 'Central768':
                self._url += '/central768'
            elif self._seldict[key] == 'Central512':
                self._url += '/central512'
            elif self._seldict[key] == 'Central256':
                self._url += '/central256'
            else:
                self._url += '/%s' % self._seldict[key]
        elif key == 'focal_plane_mask':
            # if self._seldict[key] == gmos_focal_plane_mask(self._seldict[key]):
            #     self._url += '/' + str(self._seldict[key])
            # else:
            self._url += '/mask=' + str(self._seldict[key])
        elif key == 'filepre':
            self._url += '/filepre=%s' % self._seldict[key]
        elif key == 'site_monitoring':
            if self._seldict[key] is True:
                self._url += '/site_monitoring'
            else:
                self._url += '/not_site_monitoring'
        else:
            self._url += '/%s' % self._seldict[key]

    return self._url
