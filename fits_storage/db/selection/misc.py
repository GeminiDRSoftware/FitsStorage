@property
def openquery(self):
    """
    Returns a boolean to say if the selection is limited to a reasonable number
    of results - ie does it contain a date, daterange, prog_id, obs_id etc.
    returns True if this selection will likely return a large number of results
    """

    things = {'date', 'daterange', 'night', 'nightrange','program_id',
              'observation_id', 'data_label', 'filename', 'filepre', 'filelist'}
    selection_keys = set(self._seldict)  # Makes a set out of selection.keys()

    # Are the previous two sets disjoint?
    return len(things & selection_keys) == 0




