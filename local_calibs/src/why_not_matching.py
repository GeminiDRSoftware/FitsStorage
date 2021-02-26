import sys
import logging
from os.path import basename

from recipe_system.cal_service.localmanager import LocalManager, extra_descript, args_for_cals
from recipe_system.cal_service.calrequestlib import get_cal_requests
import astrodata
import gemini_instruments
from gemini_calmgr.cal import get_cal_object

from gemini_calmgr.cal.render_query import render_query

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style
from prompt_toolkit.history import InMemoryHistory
from pygments.lexers.sql import SqlLexer


# 1. SQL query dump and prompt
# 2. Rule python structure and interpretation
# 3. File-based rule structure and interpretation
debug_mode = 3


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
        if debug_mode == 1:
            args["render_query"] = True
            cals, render_query_result = getattr(cal_obj, method)(**args)
        else:
            cals = getattr(cal_obj, method)(**args)
        for cal in cals:
            if cal.diskfile.filename == basename(calibration):
                logging.info("Calibration matched")
                exit(0)

        if debug_mode != 1:
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
        elif debug_mode == 1:
            text = ''
            sql_completer = WordCompleter([
                'abort', 'action', 'add', 'after', 'all', 'alter', 'analyze', 'and',
                'as', 'asc', 'attach', 'autoincrement', 'before', 'begin', 'between',
                'by', 'cascade', 'case', 'cast', 'check', 'collate', 'column',
                'commit', 'conflict', 'constraint', 'create', 'cross', 'current_date',
                'current_time', 'current_timestamp', 'database', 'default',
                'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct',
                'drop', 'each', 'else', 'end', 'escape', 'except', 'exclusive',
                'exists', 'explain', 'fail', 'for', 'foreign', 'from', 'full', 'glob',
                'group', 'having', 'if', 'ignore', 'immediate', 'in', 'index',
                'indexed', 'initially', 'inner', 'insert', 'instead', 'intersect',
                'into', 'is', 'isnull', 'join', 'key', 'left', 'like', 'limit',
                'match', 'natural', 'no', 'not', 'notnull', 'null', 'of', 'offset',
                'on', 'or', 'order', 'outer', 'plan', 'pragma', 'primary', 'query',
                'raise', 'recursive', 'references', 'regexp', 'reindex', 'release',
                'rename', 'replace', 'restrict', 'right', 'rollback', 'row',
                'savepoint', 'select', 'set', 'table', 'temp', 'temporary', 'then',
                'to', 'transaction', 'trigger', 'union', 'unique', 'update', 'using',
                'vacuum', 'values', 'view', 'virtual', 'when', 'where', 'with',
                'without', 'quit'], ignore_case=True)
            style = Style.from_dict({
                'completion-menu.completion': 'bg:#008888 #ffffff',
                'completion-menu.completion.current': 'bg:#00aaaa #000000',
                'scrollbar.background': 'bg:#88aaaa',
                'scrollbar.button': 'bg:#222222',
            })
            dfl = render_query_result
            history = InMemoryHistory()
            session = PromptSession(lexer=PygmentsLexer(SqlLexer), completer=sql_completer, style=style,
                                    history=history)
            while text.upper() != 'Q' and text.upper() != 'QUIT':
                try:
                    if dfl:
                        print("\n\nEditable Query:\n")
                        text = session.prompt('> ', default=dfl)
                    else:
                        text = session.prompt('> ')
                    dfl = ''
                    if text.upper() == 'Q' or text.upper() == 'QUIT':
                        exit(0)
                except KeyboardInterrupt:
                    continue
                except EOFError:
                    break
                else:
                    try:
                        text = text.replace('\n', '')
                        messages = mgr.session.execute(text)
                    except Exception as e:
                        print(repr(e))
                    else:
                        print('\n\nResults\n-------\n')
                        for message in messages:
                            print(message)

    if reasons:
        logging.info(reasons)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        logging.error("Useage: why_not_matching <filename> <cal_type> <calibrationfilename>")
    filename = sys.argv[1]
    cal_type = sys.argv[2]
    calibration = sys.argv[3]

    why_not_matching(filename, cal_type, calibration)
