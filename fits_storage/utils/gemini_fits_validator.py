'''This module provides with custom functions use by the FITS validation engine,
   that are specific to Gemini'''

try:
    from .fits_validator import *
    from .fits_validator import coerceValue, log, BadFilter
    from .gemini_metadata_utils import gemini_instrument
    from .gemini_metadata_utils import GeminiProgram, GeminiObservation, GeminiDataLabel
except ValueError:
    from fits_storage.utils.fits_validator import *
    from fits_storage.utils.fits_validator import coerceValue, log
    from .gemini_metadata_utils import gemini_instrument
    from .gemini_metadata_utils import GeminiProgram, GeminiObservation, GeminiDataLabel

import astrodata
import gemini_instruments
from datetime import datetime, timedelta
from io import StringIO
import sys
import astropy.io.fits as pf
import logging

FACILITY_INSTRUME = {'bHROS', 'F2', 'GMOS-N', 'GMOS-S', 'GNIRS', 'GPI', 'GSAOI', 'NICI', 'NIFS', 'NIRI'}
STATUSES.append('NOTGEMINI')

# This is used to determine things like if we test for IAA or OBSCLASS
# This is an initial estimate using empirical data. The value must
# be corrected at a later point.
OLDIMAGE = datetime(2007, 6, 28)
OBSCLASS_VALUES = {'dayCal',  'partnerCal',  'acqCal',  'acq',  'science',  'progCal'}

class NotGeminiData(ValidationError):
    pass

@RuleSetFactory.register_function("?gemini-data", excIfFalse = NotGeminiData)
def test_for_gemini_data(hlist, env):
    pdu = hlist[0]
    if 'XTENSION' in pdu:
        return True
    try:
        if gemini_instrument(pdu['INSTRUME'], gmos=True) is None:
            return False

        if pdu['INSTRUME'] in FACILITY_INSTRUME:
            env.features.add('facility')
        else:
            env.features.add('non-facility')

        return True

    except KeyError:
        return False

@RuleSetFactory.register_function("engineering", excIfTrue = EngineeringImage)
def engineering_image(hlist, env):
    "Naive engineering image detection"
    pdu = hlist[0]
    if pdu.get('GEMENG') is True:
        return True

    if 'XTENSION' in pdu:
        return False
    try:
        prgid = str(pdu['GEMPRGID'])
        if prgid[:2] in ('GN', 'GS') and ('ENG' in prgid.upper()):
            return True

        if check_observation_related_fields(pdu, env) is not True:
            return True, "Does not look like a valid program ID"
    except KeyError:
        return True, "Missing GEMPRGID"

    return False

# We're not using this function at al. Comment it for the time being
#
#@RuleSetFactory.register_function("calibration")
#def calibration_image(header, env):
#    "Naive calib image detection"
#    prgid = header.get('GEMPRGID', '')
#    try:
#        fromId = prgid.startswith('GN-CAL') or prgid.startswith('GS-CAL')
#    except AttributeError as e:
#        return GeneralError("Testing GEMPRGID: " + str(e))
#    return fromId or (header.get('OBSCLASS') == 'dayCal')

@RuleSetFactory.register_function("wcs-after-pdu")
def wcs_in_extensions(header, env):
    try:
        if header.get('FRAME', '').upper() in ('AZEL_TOPO', 'NO VALUE'):
            env.features.add('no-wcs-test')
    except AttributeError:
        # In some cases FRAME is not a string...
        pass

    return True

@RuleSetFactory.register_function("should-test-wcs")
def wcs_or_not(header, env):
    feat = env.features
    return (    ('facility' in feat or 'non-facility' in feat)
            and ('no-wcs-test' not in feat)
            and (   ('wcs-in-pdu' in feat and 'XTENSION' not in header)
                 or ('wcs-in-pdu' not in feat and header.get('XTENSION') == 'IMAGE')))

@RuleSetFactory.register_function("valid-observation-info", excIfFalse = EngineeringImage)
def check_observation_related_fields(header, env):
    prg = GeminiProgram(str(header['GEMPRGID']))
    obs = GeminiObservation(str(header['OBSID']))
    dl  = GeminiDataLabel(str(header['DATALAB']))

    valid = (prg.valid and obs.obsnum != '' and dl.dlnum != ''
                       and obs.obsnum == dl.obsnum
                       and prg.program_id == obs.program.program_id == dl.projectid)

    if not valid:
        return False, "Not a valid Observation ID"

    return True

@RuleSetFactory.register_function("valid-filters", excIfFalse = BadFilter)
def check_valid_filters(header, env):
    for kw in ['FILTER', 'FILTER1', 'FILTER2', 'FILTER3']:
        if kw in header:
            f = header[kw]
            if f.upper() == 'UNDEFINED':
                return False
    return True

@RuleSetFactory.register_function("valid-radesys-radecsys")
def check_valid_radesys_radecsys(header, env):
    if 'RADECSYS' in header or 'RADESYS' in header:
        return True
    return False, "Expected RADECSYS or RADESYS keyword"

@RuleSetFactory.register_function('set-date')
def set_date(header, env):
    bogus = False
    for kw in ('DATE-OBS', 'DATE'):
        try:
            coerceValue(header[kw])
            env.features.add('date:' + header[kw])
            return True
        except KeyError:
            pass
        except ValueError:
            bogus = True

    if 'MJD_OBS' in header and header['MJD_OBS'] != 0.:
        mjdzero = datetime(1858, 11, 17, 0, 0, 0, 0, None)
        mjddelta = timedelta(header['MJD_OBS'])
        mjddt = mjdzero + mjddelta
        d = mjddt.strftime('%Y-%m-%d')
        env.features.add('date:' + d)
        return True

    if not bogus:
        return False, "Can't find DATE/DATE-OBS to set the date"
    else:
        return False, "DATE/DATE-OBS contains bogus info"

@RuleSetFactory.register_function('failed-data', excIfTrue=BadData)
def check_for_bad_RAWGEMWA(hlist, env):
    return hlist[0].get('RAWGEMQA', '') == 'BAD'

class AstroDataEvaluator(Evaluator):
    def __init__(self, *args, **kw):
        super(AstroDataEvaluator, self).__init__(*args, **kw)

    def _set_initial_features(self, fits, tags):
        s = set()
        if 'PREPARED' in tags:
            s.add('prepared')

        return s

    def evaluate(self, ad_object):
        try:
            # Opens the raw FITS file and sends it to 
            return super(AstroDataEvaluator, self).evaluate(
                    pf.open(ad_object.path, memmap=True, do_not_scale_image_data=True, mode='readonly'),
                    ad_object.tags)
        except NotGeminiData:
            return Result(False, 'NOTGEMINI', "This doesn't look at all like data produced at Gemini")
        except BadFilter:
            return Result(False, 'NOPASS', "Bad filter (one or more FILTER* keywords are bad)")
        except BadData:
            return Result(True,  'BAD', "Bad data (RAWGEMQA = BAD)")

def process_argument(argv, argument):
    try:
        argv.remove(argument)
        return True
    except ValueError:
        return False

if __name__ == '__main__':
    argv = sys.argv[1:]
    debug   = process_argument(argv, '-d')
    verbose = process_argument(argv, '-v')
    use_ad  = process_argument(argv, '-a')

    if debug:
        logging.basicConfig(level=logging.DEBUG)

    try:
        fits = pf.open(argv[0])
    except IndexError:
        fits = pf.open(StringIO(sys.stdin.read()))
    fits.verify('fix+exception')

    if verbose:
        try:
            env = Environment()
            env.features = set()
            rs = RuleCollection()
            rs.initialize('fits')
            err = 0
            for n, hdu in enumerate(fits):
                env.hduNum = n
                log("* Testing HDU {0}".format(n))
                res, args = rs.test(hdu.header, env)
                if not res:
                    err += 1
                    for message in args:
                        log("   - {0}".format(message))
                elif 'failed' in env.features:
                    log("  Failed data")
                    break
                elif not args:
                    err += 1
                    log("  No key ruleset found for this HDU")

        except EngineeringImage as exc:
            s = str(exc)
            if not s:
                log("Its an engineering image")
            else:
                log("Its an engineering image: {0}".format(s))
            err = 0
        except NoDateError:
            log("This image has no recognizable date")
            err = 1
        except NotGeminiData:
            log("This doesn't look like Gemini data")
            err = 0
        except BadData:
            log("Failed data")
            err = 0
        except RuntimeError as e:
            log(str(e))
            err = 1
        if err > 0:
            sys.exit(-1)
    else:
        if use_ad:
            evaluate = AstroDataEvaluator()
            # result = evaluate(astrodata.open(fits))
            result = evaluate(astrodata.open(argv[0]))
        else:
            evaluate = Evaluator()
            result = evaluate(fits)
        if result.message is not None:
            print((result.message))
    sys.exit(0)
