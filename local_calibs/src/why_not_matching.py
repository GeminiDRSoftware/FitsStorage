import sys
import logging
from os.path import basename

from recipe_system.cal_service.localmanager import LocalManager, extra_descript, args_for_cals
from recipe_system.cal_service.calrequestlib import get_cal_requests
import astrodata
import gemini_instruments
from gemini_calmgr.cal import get_cal_object


def build_descripts(rq):
    descripts = rq.descriptors
    for (type_, desc) in list(extra_descript.items()):
        descripts[desc] = type_ in rq.tags
    return descripts


def why_not_matching(filename, cal_type, calibration):
    try:
        filead = astrodata.open(filename)
    except:
        logging.error(f"Unable to open {filename} with DRAGONS")
        exit(1)
    try:
        calad = astrodata.open(calibration)
    except:
        logging.error(f"Unable to open {calibration} with DRAGONS")
        exit(2)
    try:
        mgr = LocalManager(":memory:")
        mgr.init_database(wipe=True)
    except:
        logging.error("Unable to setup in-memory calibration manager")
        exit(3)
    try:
        mgr.ingest_file(calibration)
    except:
        logging.error("Unable to ingest calibration file")
        exit(4)

    rqs = get_cal_requests([filead,], cal_type, procmode=None)
    if not rqs:
        logging.error("Unexpected error creating cal requests")
        exit(5)

    reasons = list()
    for idx in range(len(rqs)):
        rq = rqs[idx]
        descripts = build_descripts(rq)
        types = rq.tags
        cal_obj = get_cal_object(mgr.session, filename=None, header=None,
                                 descriptors=descripts, types=types, procmode=rq.procmode)
        method, args = args_for_cals.get(cal_type, (cal_type, {}))

        # Obtain a list of calibrations and check if we matched
        cals = getattr(cal_obj, method)(**args)
        for cal in cals:
            if cal.diskfile.filename == basename(calibration):
                logging.info("Calibration matched")
                exit(0)

        if method.startswith('processed_'):
            processed = True
            method = method[10:]
        else:
            processed = False
        if hasattr(cal_obj, 'why_not_matching'):
            chk = cal_obj.why_not_matching(basename(calibration), method, processed)
            if chk is not None:
                reasons.append(chk)
        else:
            logging.warning("Calibration match checking not available for %s" % cal_obj.__class__)
    if reasons:
        logging.info(reasons)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        logging.error("Useage: why_not_matching <filename> <cal_type> <calibrationfilename>")
    filename = sys.argv[1]
    cal_type = sys.argv[2]
    calibration = sys.argv[3]

    why_not_matching(filename, cal_type, calibration)
