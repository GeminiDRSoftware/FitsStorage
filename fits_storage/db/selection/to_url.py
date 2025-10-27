import urllib.parse
import urllib.error

import fits_storage.gemini_metadata_utils as gmu


def to_url(self, with_columns=False):
    """
    Receives a selection dictionary, parses values and converts to URL string
    """
    self._url = ''

    # Pack defaults into 'defaults' item if appropriate. Note whether we
    # modified the selection here, so that we can leave it in the same packed
    # state as we found it.
    packed = self.packdefaults()

    # We only want one of data_label, observation_id, program_id in the URL,
    # the most specific one should carry.
    if 'data_label' in self:
        self.pop('observation_id', None)
        self.pop('program_id', None)
    if 'observation_id' in self:
        self.pop('program_id', None)

    # We don't want daterange or nightrange if date or night is present
    if 'date' in self:
        self.pop('daterange', None)
    if 'night' in self:
        self.pop('nightrange', None)

    # Handle defaults separately, so it always ends up at the front of the URL
    if self.get('defaults') is True:
        self._url += '/defaults'

    # Run through the selection keys in a defined order here, so that the
    # resulting URL doesn't depend on the order they're in the dict, as that
    # will depend on what order they were added.
    for key in sorted(self.keys()):
        if key in {'warning', 'Search', 'ObsLogsOnly'}:
            # Don't put the warning text or search buttons in the URL
            pass
        elif key == 'defaults':
            # This was handled up front. Need this elif here to stop it being
            # parsed by the default case later
            pass
        elif key == 'data_label':
            # See if it is a valid data_label
            dl = gmu.GeminiDataLabel(self[key])
            if dl.valid:
                # Regular form, just stuff it in
                self._url += '/%s' % self[key]
            else:
                # It's a non-standard one
                self._url += '/datalabel=%s' % self[key]
        elif key == 'observation_id':
            # See if it is a valid observation id, or if we need to add obsid=
            go = gmu.GeminiObservation(self[key])
            if go.valid:
                # Regular obs id, just stuff it in
                self._url += '/%s' % self[key]
            else:
                # It's a non-standard one
                self._url += '/obsid=%s' % self[key]
        elif key == 'program_id':
            # See if it is a valid program id, or if we need to add progid=
            gp = gmu.GeminiProgram(self[key])
            if gp.valid:
                # Regular program id, just stuff it in
                self._url += '/%s' % self[key]
            else:
                # It's a non-standard one
                self._url += '/progid=%s' % self[key]
        elif key == 'object':
            # We custom escape '/' characters here as if we use standard url
            # escping, the webserver/wsgi layer (outside our control) will
            # de-escape it for us and we'll be left with / in the name that we
            # can't differentiate from path separators. '/' needs special
            # handling because we do an explicit split on '/' in the router
            # code to generate the url things list.
            self._url += '/object=%s' % self[key].replace('/', '=slash=')
        elif key == 'publication':
            # Need to handle '&' characters in bibcodes, but not '/'s
            self._url += '/publication=%s' % urllib.parse.quote(self[key])
        elif key == 'spectroscopy':
            if self[key] is True:
                self._url += '/spectroscopy'
            else:
                self._url += '/imaging'
        elif key in {'ra', 'dec', 'sr', 'filter', 'cenwlen', 'disperser',
                     'camera', 'exposure_time', 'coadds', 'pupil_mask',
                     'PIname', 'ProgramText', 'gain', 'readspeed', 'welldepth',
                     'readmode', 'date', 'daterange', 'night', 'nightrange'}:
            self._url += '/%s=%s' % (key, self[key])
        elif key == 'cols':
            if with_columns:
                self._url += '/cols=%s' % self['cols']
        elif key == 'present':
            if self[key] is True:
                self._url += '/present'
            else:
                self._url += '/notpresent'
        elif key == 'canonical':
            if self[key] is True:
                self._url += '/canonical'
            else:
                self._url += '/notcanonical'
        elif key == 'twilight':
            if self[key] is True:
                self._url += '/twilight'
            else:
                self._url += '/nottwilight'
        elif key == 'engineering':
            if self[key] is True:
                self._url += '/engineering'
            elif self[key] is False:
                self._url += '/notengineering'
            else:
                self._url += '/includeengineering'
        elif key == 'calprog':
            if self[key] is True:
                self._url += '/calprog'
            elif self[key] is False:
                self._url += '/notcalprog'
        elif key == 'science_verification':
            if self[key] is True:
                self._url += '/science_verification'
            else:
                self._url += '/notscience_verification'
        elif key == 'detector_roi':
            if self[key] == 'Full Frame':
                self._url += '/fullframe'
            elif self[key] == 'Central Spectrum':
                self._url += '/centralspectrum'
            elif self[key] == 'Central Stamp':
                self._url += '/centralstamp'
            elif self[key] == 'Central768':
                self._url += '/central768'
            elif self[key] == 'Central512':
                self._url += '/central512'
            elif self[key] == 'Central256':
                self._url += '/central256'
            else:
                self._url += '/%s' % self[key]
        elif key == 'focal_plane_mask':
            # if self[key] == gmos_focal_plane_mask(self[key]):
            #     self._url += '/' + str(self[key])
            # else:
            self._url += '/mask=' + str(self[key])
        elif key == 'filepre':
            self._url += '/filepre=%s' % self[key]
        elif key == 'path':
            self._url += '/path=%s' % self[key]
        elif key == 'site_monitoring':
            if self[key] is True:
                self._url += '/site_monitoring'
            else:
                self._url += '/not_site_monitoring'
        elif key == 'pre_image':
            if self[key] is True:
                self._url += '/preimage'
        elif key == 'processing_tag':
            self._url += '/processing_tag=%s' % self[key]
        else:
            self._url += '/%s' % self[key]

    # Leave the selection in the same 'packed' state as we found it.
    if packed:
        self.unpackdefaults()

    return self._url
