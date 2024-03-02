"""
This module holds the CalibrationGHOST class
"""
import functools

from fits_storage.core.orm.header import Header
from fits_storage.cal.orm.ghost import Ghost

from fits_storage.cal.calibration.calibration import Calibration, \
    not_processed, CalQuery


class CalibrationGHOST(Calibration):
    """
    This class implements a calibration manager for GHOST.
    It is a subclass of Calibration
    """
    instrClass = Ghost
    instrDescriptors = [
        'arm',
        'detector_x_bin',
        'detector_y_bin',
        'read_speed_setting',
        'gain_setting',
        'prepared',
        'overscan_trimmed',
        'overscan_subtracted',
        'exposure_time',
        'focal_plane_mask',
        'want_before_arc',
        ]

    def __init__(self, session, header, descriptors, types, procmode=None):

        # If descriptors is None - ie we are reassembling the descriptor
        # values from the database, the ghost bundles require special handling.

        # If we have a ghost bundle, we call the parent class __init__, but
        # tell it not to try and do any instrument specific setup, as we
        # need special handling here to load multiple rows from the ghost table
        # and reassemble them into dictionaries for the instrument specific
        # descriptors.

        # If we do not have a bundle, the regular calibration class __init__
        # is fine.

        if descriptors is None:
            # Get the ghost table rows for this header id
            ic = self.instrClass
            query = session.query(ic).filter(ic.header_id == header.id)
            instrows = query.all()

            if instrows == 0:
                raise ValueError("No Ghost rows found for header id %d" %
                                 header.id)
            elif instrows == 1:
                # Not a bundle, just call the regular init, which will pick up
                # the instrDescriptors from above.
                super(CalibrationGHOST, self).\
                    __init__(session, header, descriptors, types, procmode)
            else:
                # We have a bundle.
                super(CalibrationGHOST, self).\
                    __init__(session, header, descriptors, types, procmode,
                             instinit=False)
                # Now go through the instrument descriptors instrows and
                # reassemble dictionaries for them...

                for desc in self.instrDescriptors:
                    d = {}
                    for row in instrows:
                        d[row.arm] = getattr(row, desc)
                    self.descriptors[desc] = d

                # Quick kludge here that in a bundle, arm is None
                self.descriptors['arm'] = None

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

            # PROCESSED_SCIENCE files do not require anything
            if 'PROCESSED_SCIENCE' in self.types:
                return

            # Do BIAS. Most things require Biases.
            if self.descriptors['observation_type'] != 'BIAS':
                self.applicable.append('bias')
                self.applicable.append('processed_bias')

            # If it (is spectroscopy) and * Note: tweaked for GHOST to ignore
            # flag, it's basically always spectroscopy
            # * TBD how/what to change in the AstroDataGhost for DRAGONS master
            # (is an OBJECT) and
            # (is not a Twilight) and
            # (is not a specphot)
            # then it needs an arc, flat, spectwilight, specphot
            if (  # (self.descriptors['spectroscopy'] == True) and
                    (self.descriptors['observation_type'] == 'OBJECT') and
                    (self.descriptors['object'] != 'Twilight') and
                    (self.descriptors['observation_class'] not in
                     ['partnerCal', 'progCal'])):
                self.applicable.append('arc')
                self.applicable.append('processed_arc')
                self.applicable.append('flat')
                self.applicable.append('processed_flat')
                self.applicable.append('spectwilight')
                self.applicable.append('specphot')

    # @not_imaging
    def arc(self, processed=False, howmany=2):
        """
        This method identifies the best GHOST ARC to use for the target
        dataset.

        This will find GHOST arcs. If "want_before_arc" is set and true,
        it limits to 1 result and only matches observations prior to the
        ut_datetime.  If it is set and false, it limits to 1 result after the
        ut_datetime.  Otherwise, it keeps the `howmany` as specified with a
        default of 2 and has no restriction on ut_datetime. It matches within
        1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw arcs
        howmany : int, default 2 if `want_before_arc` is not set, or 1 if it is
            How many matches to return

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match
        """
        ab = self.descriptors.get('want_before_arc', None)
        # Default 2 arcs, hopefully one before and one after
        if ab is not None:
            howmany = 1
        else:
            howmany = howmany if howmany else 2
        filters = []

        if ab:
            # Add the 'before' filter
            filters.append(Header.ut_datetime < self.descriptors['ut_datetime'])
        elif ab is None:
            # No action required
            pass
        else:
            # Add the after filter
            filters.append(Header.ut_datetime > self.descriptors['ut_datetime'])

        query = self.get_query()\
            .arc(processed)\
            .add_filters(*filters)\
            .match_descriptors(Header.instrument,
                               Header.focal_plane_mask)\
            .max_interval(days=365)

        return query.all(howmany)

    def bias(self, processed=False, howmany=None):
        """
        Method to find the best bias frames for the target dataset

        This will find GHOST biases with matching arm, read speed setting,
        gain setting, and x and y binning. If it's 'prepared' data, it will
        match overscan trimmed and overscan subtracted.

        It matches within 90 days

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw biases
        howmany : int, default 1 if processed, else 50

        Returns
        -------
        list of :class:`fits_storage.orm.header.Header`
        records that match the criteria
        """

        if howmany is None:
            howmany = 1 if processed else 50

        filters = []

        # The Overscan section handling: this only applies to processed
        # biases as raw biases will never be overscan trimmed or subtracted,
        # and if they're processing their own biases, they ought to know what
        # they want to do.
        if processed:
            # if self.descriptors['prepared']:
            # By definition, if it's a bundle, it's not prepared.
            pd = self.descriptors['prepared']
            if not isinstance(pd, dict) and pd:
                # If the target frame is prepared, match the overscan state.
                filters.append(Ghost.overscan_trimmed ==
                               self.descriptors['overscan_trimmed'])
                filters.append(Ghost.overscan_subtracted ==
                               self.descriptors['overscan_subtracted'])
            else:
                # If the target frame is not prepared, then we don't know
                # what their processing intentions are. we could go with the
                # default (which is trimmed and subtracted). But actually
                # it's better to just send them all we have.
                pass

        query = self.get_query()\
            .bias(processed=processed)\
            .add_filters(*filters)\
            .match_descriptors(Header.instrument,
                               Ghost.arm,
                               Ghost.detector_x_bin,
                               Ghost.detector_y_bin,
                               Ghost.read_speed_setting,
                               Ghost.gain_setting)\
            .max_interval(days=90)

        return query.all(howmany)

    def imaging_flat(self, processed, howmany, flat_descr, filt, sf=False):
        """
        Method to find the best imaging flats for the target dataset

        This will find imaging flats that are either observation type of
        'FLAT' or are both dayCal and 'Twilight'.  This also adds a large set
        of flat filters in flat_descr from the higher level flat query.

        This will find GHOST imaging flats with matching read speed setting,
        gain setting, focal plane mask

        It matches within 180 days

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw imaging flats
        howmany : int, default 1 if processed, else 20
            How many do we want results
        flat_descr: list
            set of filter parameters from the higher level function calling
            into this helper method
        filt: list
            Additional filter terms to apply from the higher level method
        sf: bool
            True for slit flats, else False

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header`
            records that match the criteria
        """

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

        query = query.add_filters(*filt)\
            .match_descriptors(*flat_descr)\
            .max_interval(days=180)

        return query.all(howmany)

    def spectroscopy_flat(self, processed, howmany, flat_descr, filt):
        """
        Method to find the best imaging flats for the target dataset

        It matches within 180 days

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw imaging flats
        howmany : int, default 1 if processed, else 2
            How many do we want results
        flat_descr: list
            set of filter parameters from the higher level function calling
            into this helper method
        filt: list
            Additional filter terms to apply from the higher level method

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header`
            records that match the criteria
        """

        if howmany is None:
            howmany = 1 if processed else 2

        query = self.get_query()\
            .flat(processed)\
            .add_filters(*filt)\
            .match_descriptors(*flat_descr)\
            .max_interval(days=180)

        return query.all(howmany)

    def flat(self, processed=False, howmany=None):
        """
        Method to find the best GHOST FLAT fields for the target dataset

        This will find GHOST flats with matching arm, read speed setting,
        gain setting, focal plane mask.  It will search for matching
        spectroscopy setting and matching amp read area.  Then additional
        filtering is done based on logic either for imaging flats or
        spectroscopy flats, as per :meth:`spectroscopy_flat` and
        :meth:`imaging_flat`.

        It matches within 180 days

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw imaging flats
        howmany : int
            How many do we want

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header`
            records that match the criteria
        """
        filters = []

        # Common descriptors for both types of flat
        # Must totally match instrument, detector_x_bin, detector_y_bin
        flat_descriptors = [
            Header.instrument,
            Header.spectroscopy,
            Ghost.read_speed_setting,
            Ghost.gain_setting,
            Ghost.focal_plane_mask,
            ]

        return self.spectroscopy_flat(processed, howmany, flat_descriptors,
                                      filters)

    def processed_slitflat(self, howmany=None):
        """
        Method to find the best GHOST SLITFLAT for the target dataset

        If the type is 'SLITV', this method falls back to the regular
        :meth:`flat` logic.

        This will find GHOST imaging flats with matching read speed setting,
        gain setting, filter name, res mode, and disperser.  It filters
        further on the logic in :meth:`imaging_flat`.

        It matches within 180 days

        Parameters
        ----------

        howmany : int, default 1
            How many do we want results

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match
            the criteria
        """

        if 'SLITV' in self.types:
            return self.flat(True, howmany)

        filters = []
        filters.append(Header.spectroscopy == False)

        # Common descriptors for both types of flat
        # Must totally match instrument, detector_x_bin, detector_y_bin, filter
        flat_descriptors = [
            Header.instrument,
            Ghost.read_speed_setting,
            Ghost.gain_setting,
            Ghost.focal_plane_mask,
            ]

        return self.imaging_flat(False, howmany, flat_descriptors, filters,
                                 sf=True)

    def processed_slit(self, howmany=None):
        """
        Method to find the best processed GHOST SLIT for the target dataset

        This will find GHOST processed slits with a 'Sony-ICX674' detector.
        It matches the observation type, res mode, and within 30 seconds.
        For 'ARC' observation type it matches 'PROCESSED_UNKNOWN' data,
        otherwise it matches 'PREPARED' data.

        Parameters
        ----------

        howmany : int
            How many do we want results

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header`
            records that match the criteria
        """

        descripts = (
            Header.instrument,
            Header.observation_type,
            Ghost.focal_plane_mask,
            )

        filters = (
            Ghost.detector_name.contains('ICX674'),
        )

        # this may change pending feedback from Kathleen
        red = 'PROCESSED_ARC' if self.descriptors['observation_type'] == \
                                 'ARC' else 'PROCESSED_UNKNOWN'

        query = self.get_query()\
            .reduction(red)\
            .spectroscopy(False)\
            .match_descriptors(*descripts)\
            .add_filters(*filters) \
            .max_interval(seconds=30)
            # Need the slit image that matches the input observation;
            # needs to match within 30 seconds!

        return query.all(howmany)

    # We don't handle processed ones (yet)
    @not_processed
    # @not_imaging
    def spectwilight(self, processed=False, howmany=None, return_query=False):
        """
        Method to find the best spectwilight - ie spectroscopy twilight
        ie MOS / IFU / LS twilight

        This will find GHOST spec twilights matching
        focal plane mask, and x and y binning.
        It matches within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw spec twilights
        howmany : int, default 2
            How many do we want results

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header`
            records that match the criteria
        """
        # Default number to associate
        howmany = howmany if howmany else 2

        filters = []

        query = (
            self.get_query()
                # They are OBJECT spectroscopy frames with target twilight
                .raw().OBJECT().spectroscopy(True).object('Twilight')
                .add_filters(*filters)
                .match_descriptors(Header.instrument,
                                   Ghost.detector_x_bin,
                                   Ghost.detector_y_bin,
                                   Ghost.focal_plane_mask)
                .max_interval(days=365)
            )
        return query.all(howmany)

    # We don't handle processed ones (yet)
    @not_processed
    # @not_imaging
    def specphot(self, processed=False, howmany=None, return_query=False):
        """
        Method to find the best specphot observation

        The data must be partnerCal or progCal and not be Twilight.

        It matches within 1 year.

        Parameters
        ----------

        processed : bool
            Indicate if we want to retrieve processed or raw specphot
        howmany : int, default 2
            How many do we want results

        Returns
        -------
            list of :class:`fits_storage.orm.header.Header` records that match
        """
        # Default number to associate
        howmany = howmany if howmany else 4

        filters = []

        query = (
            self.get_query()
                # They are OBJECT partnerCal or progCal spectroscopy frames with target not twilight
                .raw().OBJECT()  # .spectroscopy(True) * per above, they are all spectroscopy
                .add_filters(Header.observation_class.in_(['partnerCal', 'progCal']),
                             Header.object != 'Twilight',
                             *filters)
                # Found lots of examples where detector binning does not match, so we're not adding those
                .match_descriptors(Header.instrument,
                                   )
            )
        return query.all(howmany)

    def get_query(self):
        """
        Returns an ``CalQuery`` object, populated with the current session,
        instrument class, descriptors and the setting for full/not-full query.

        If the descriptors are from a ghost 'bundle' we need to return a
        special GHOSTCalQuery instance. If they are not from a bundle, we
        return a regular CalQuery instance.

        """

        if self.descriptors['arm'] is None:
            return GHOSTCalQuery(self.session, self.instrClass, self.descriptors,
                                 procmode=self.procmode)
        else:
            return CalQuery(self.session, self.instrClass, self.descriptors,
                            procmode=self.procmode)

class GHOSTCalQuery(object):
    """
    This is a special case calquery class for GHOST, however it is not
    actually a subclass of CalQuery. The regular CalQuery class works fine
    for all GHOST files *except* for bundles. For bundles, we have to make a
    separate CalQuery instance for each arm in the bundle (as each arm has
    its own entry in the ghost table). We have to apply any filters or
    required by the CalibrationGHOST class to all of these, and then we have to
    run all the queries and concatenate the results.
    """
    def __init__(self, session, instrClass, descriptors, procmode=None):
        self.descriptors = descriptors
        self.procmode = procmode

        # We don't have ad.tags to check for 'BUNDLE' at this point, so we'll
        # use the arm descriptor being None as a proxy.
        if descriptors['arm'] is not None:
            raise ValueError("Tried to instantiate GHOSTCalQuery on non-bundle")

        # We need to make a pseudo 'descriptors' dict for each arm...
        self.pdescriptors = {}

        # There's no real good way to get a list of arms present at this, so
        # we use the exposure_time descriptor as a good example.
        arms = descriptors['exposure_time'].keys()

        for arm in arms:
            self.pdescriptors[arm] = {}
            for key in descriptors.keys():
                if isinstance(descriptors[key], dict):
                    self.pdescriptors[arm][key] = descriptors[key][arm]
                else:
                    self.pdescriptors[arm][key] = descriptors[key]
                # And that will of course have set arm to None, so fix that
                self.pdescriptors[arm]['arm'] = arm

        # Now loop again and make the list of calqueries. This doesn't need to
        # be a dict as we can tell from the arm descriptor in each one which it
        # is if we ever need to.
        self.calqueries = []
        for arm in arms:
            self.calqueries.append(CalQuery(session, instrClass,
                                            self.pdescriptors[arm], procmode))
        # Set up the "call through methods" here
        calmethods = ['bias', 'arc', 'flat']
        argsmethods = ['add_filters', 'match_descriptors', 'raw', 'OBJECT',
                       'spectroscopy', 'object']
        kwmethods = ['max_interval']
        for m in calmethods:
            setattr(self, m, functools.partial(self.docal, m))
        for m in argsmethods:
            setattr(self, m, functools.partial(self.doarg, m))
        for m in kwmethods:
            setattr(self, m, functools.partial(self.dokw, m))

    def all(self, *args, **kwargs):
        """
        Calls .all() on each of the list of calqueries and returns a deduped
        list. There may be some foibles with ordering here, that we're going
        to gloss over for now...
        """

        headers = set()
        for cq in self.calqueries:
            newheaders=cq.all(*args, **kwargs)
            headers = headers.union(newheaders)

        return headers

    def docal(self, cal, processed=False):
        newcqs = []
        for cq in self.calqueries:
            newcqs.append(getattr(cq, cal)(processed=processed))
        self.calqueries = newcqs
        return self

    def doarg(self, thing, *args):
        newcqs = []
        for cq in self.calqueries:
            newcqs.append(getattr(cq, thing)(*args))
        self.calqueries = newcqs
        return self

    def dokw(self, thing, **kw):
        newcqs = []
        for cq in self.calqueries:
            newcqs.append(getattr(cq, thing)(**kw))
        self.calqueries = newcqs
        return self
