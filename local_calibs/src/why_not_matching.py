import sys
import logging
from os.path import basename

from recipe_system.cal_service.localmanager import LocalManager, extra_descript, args_for_cals
from recipe_system.cal_service.calrequestlib import get_cal_requests
import astrodata
import gemini_instruments
from gemini_calmgr.cal import get_cal_object

from gemini_calmgr.cal.render_query import render_query

from sqlalchemy.sql.elements import BooleanClauseList, BinaryExpression, True_
from gemini_calmgr.orm.header import Header
from gemini_calmgr.orm.diskfile import DiskFile
from gemini_calmgr.orm.gmos import Gmos


# def debug_boolean_clause_list(clause, cal_obj, header, diskfile, instr):
#     for clause in clause.clauses:
#         debug_dispatch(clause, cal_obj, header, diskfile, instr)


def show_line(table_name, key, cal_value, value, expr):
    if (not isinstance(cal_value, str) or len(cal_value) <= 28) \
            and (not isinstance(value, str) or len(value) <= 28):
        print("%9s | %18s | %30s | %30s | %s" % (table_name, key, cal_value, value, expr))
    else:
        print("%9s | %18s | cal: %58s | %s" % (table_name, key, cal_value, expr))
        print("%9s | %18s | val: %58s | %s" % ('', '', value, ''))


def debug_binary_expression(clause, cal_obj, header, diskfile, instr):
    if hasattr(clause.left, 'table'):  # isinstance(clause.left, AnnotatedColumn):
        table = clause.left.table
        key = clause.left.key
        val = clause.right.value if hasattr(clause.right, 'value') else None
        if val is None:
            if hasattr(clause.right, 'clauses') and len(clause.right.clauses) > 0:
                vals = []
                for cl in clause.right.clauses:
                    if hasattr(cl, 'value') and cl.value is not None:
                        vals.append("%s" % cl.value)
                val = ', '.join(vals)
            else:
                val = ''
        expr = "%s" % clause
        if table.name == 'header':
            show_line(table.name, key, getattr(header, key), val, expr)
        if table.name == 'diskfile':
            show_line(table.name, key, getattr(diskfile, key), val, expr)
        if table.name == 'gmos':
            show_line(table.name, key, getattr(instr, key), val, expr)


def debug_dispatch(clause, cal_obj, header, diskfile, instr):
    if isinstance(clause, BooleanClauseList):
        debug_boolean_clause_list(clause, cal_obj, header, diskfile, instr)
    elif isinstance(clause, BinaryExpression):
        debug_binary_expression(clause, cal_obj, header, diskfile, instr)


def debug_parser(query, cal_obj, header, diskfile, instr):
    for clause in query.query.whereclause.clauses:
        debug_dispatch(clause, cal_obj, header, diskfile, instr)


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
        astrodata.open(calibration)
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
        args["return_query"] = True
        cals, query_result = getattr(cal_obj, method)(**args)

        for cal in cals:
            if cal.diskfile.filename == basename(calibration):
                logging.info("Calibration matched")
                exit(0)

        header = mgr.session.query(Header).first()
        diskfile = mgr.session.query(DiskFile).first()
        instr = mgr.session.query(Gmos).first()
        print('Relevant fields from calibration:\n')
        print('Table     | Key                | Cal Value                      '
              '| Value                          | Expr')
        print('----------+--------------------+--------------------------------'
              '+--------------------------------+-------------------')
        debug_parser(query_result, cal_obj, header, diskfile, instr)

    if reasons:
        logging.info(reasons)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        logging.error("Useage: why_not_matching <filename> <cal_type> <calibrationfilename>")
    filename = sys.argv[1]
    cal_type = sys.argv[2]
    calibration = sys.argv[3]

    why_not_matching(filename, cal_type, calibration)
