@property
def openquery(self):
    """
    Returns a boolean to say if the selection is limited to a reasonable number
    of results - ie does it contain a date, daterange, prog_id, obs_id etc.
    returns True if this selection will likely return a large number of results
    """

    things = {'date', 'daterange', 'night', 'nightrange', 'program_id',
              'observation_id', 'data_label', 'filename', 'filepre', 'filelist'}
    selection_keys = set(self)  # Makes a set out of selection.keys()

    # Are the previous two sets disjoint?
    return len(things & selection_keys) == 0


def unpackdefaults(self):
    """
    If defaults=True is present in the selection dictionary, replace it with
    what that actually means.

    This is currently only applicable to use in /searchform, but could be
    expanded to other endpoints too if desired.

    Return True if we modified the dict, False otherwise.
    """

    # If defaults is present and set to True
    if self.get('defaults') is True:
        # Remove 'defaults' dictionary entry
        self.pop('defaults')
        # Set the default things
        self['engineering'] = False
        self['site_monitoring'] = False
        self['qa_state'] = 'NotFail'
        self['cols'] = 'CTOWBEQ'
        self['processing_tag'] = 'default'
        return True
    return False


def packdefaults(self):
    """
    If the selection dict has all the settings that correspond to 'defaults'
    set in their default state, remove them and replace them with a single
    'defaults': True entry.

    Return True if we modified the selection dict, False otherwise.
    """
    default = self.get('engineering') is False and \
              self.get('site_monitoring') is False and \
              self.get('qa_state') == 'NotFail' and \
              self.get('cols') == 'CTOWBEQ' and \
              self.get('processing_tag') == 'default'

    if default:
        self.pop('engineering')
        self.pop('site_monitoring')
        self.pop('qa_state')
        self.pop('cols')
        self.pop('processing_tag')
        self['defaults'] = True
        return True
    return False
