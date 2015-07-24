'''This module provides with custom functions use by the FITS validation engine,
   that are specific to Gemini'''

from .fits_validator import *
from .fits_validator import coerceValue
from ..gemini_metadata_utils import gemini_instrument
from ..gemini_metadata_utils import GeminiProgram, GeminiObservation, GeminiDataLabel

from astrodata import AstroData
from datetime import datetime, timedelta
from StringIO import StringIO

FACILITY_INSTRUME = {'bHROS', 'F2', 'GMOS-N', 'GMOS-S', 'GNIRS', 'GPI', 'GSAOI', 'NICI', 'NIFS', 'NIRI'}
STATUSES.append('NOTGEMINI')

# This is used to determine things like if we test for IAA or OBSCLASS
# This is an initial estimate using empirical data. The value must
# be corrected at a later point.
OLDIMAGE = datetime(2007, 06, 28)
OBSCLASS_VALUES = {'dayCal',  'partnerCal',  'acqCal',  'acq',  'science',  'progCal'}

class NotGeminiData(ValidationError):
    pass

@RuleSet.register_function("is-gemini-data", excIfFalse = NotGeminiData)
def test_for_gemini_data(header, env):
    if 'XTENSION' in header:
        return True
    try:
        if gemini_instrument(header['INSTRUME'], gmos=True) is None:
            return False

        if header['INSTRUME'] in FACILITY_INSTRUME:
            env.features.add('facility')
        else:
            env.features.add('non-facility')

        return True

    except KeyError:
        return False

@RuleSet.register_function("engineering", excIfTrue = EngineeringImage)
def engineering_image(header, env):
    "Naive engineering image detection"
    if header.get('GEMENG') is True:
        return True

    if 'XTENSION' in header:
        return False
    try:
        prgid = str(header['GEMPRGID'])
        if prgid[:2] in ('GN', 'GS') and ('ENG' in prgid.upper()):
            return True

        if check_observation_related_fields(header, env) is not True:
            return True, "Does not look like a valid program ID"
    except KeyError:
        return True, "Missing GEMPRGID"

    return False

@RuleSet.register_function("calibration")
def calibration_image(header, env):
    "Naive calib image detection"
    prgid = header.get('GEMPRGID', '')
    try:
        fromId = prgid.startswith('GN-CAL') or prgid.startswith('GS-CAL')
    except AttributeError as e:
        return GeneralError("Testing GEMPRGID: " + str(e))
    return fromId or (header.get('OBSCLASS') == 'dayCal')

@RuleSet.register_function("wcs-after-pdu")
def wcs_in_extensions(header, env):
    try:
        if header.get('FRAME', '').upper() in ('AZEL_TOPO', 'NO VALUE'):
            env.features.add('no-wcs-test')
    except AttributeError:
        # In some cases FRAME is not a string...
        pass

    return True

@RuleSet.register_function("should-test-wcs")
def wcs_or_not(header, env):
    feat = env.features
    return (    ('facility' in feat or 'non-facility' in feat)
            and ('no-wcs-test' not in feat)
            and (   ('wcs-in-pdu' in feat and 'XTENSION' not in header)
                 or ('wcs-in-pdu' not in feat and header.get('XTENSION') == 'IMAGE')))

@RuleSet.register_function("valid-observation-info", excIfFalse = EngineeringImage)
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

@RuleSet.register_function('set-date')
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

@RuleSet.register_function('failed-data', excIfTrue=BadData)
def check_for_bad_RAWGEMWA(header, env):
    return header.get('RAWGEMQA', '') == 'BAD'

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
            return super(AstroDataEvaluator, self).evaluate(ad_object.hdulist, ad_object.types)
        except NotGeminiData:
            return Result(False, 'NOTGEMINI', "This doesn't look at all like data produced at Gemini")
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
    verbose = process_argument(argv, '-v')
    use_ad  = process_argument(argv, '-a')

    try:
        fits = pf.open(argv[0])
    except IndexError:
        fits = pf.open(StringIO(sys.stdin.read()))
    fits.verify('fix+exception')

    if verbose:
        DEBUG = True
        try:
            env = Environment()
            env.features = set()
            rs = RuleStack()
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
            result = evaluate(AstroData(fits))
        else:
            evaluate = Evaluator()
            result = evaluate(fits)
        if result.message is not None:
            print(result.message)
    sys.exit(0)
