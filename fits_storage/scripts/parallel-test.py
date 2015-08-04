#!/usr/bin/env python

from __future__ import print_function

__doc__ = """Parallel Databased Test

If <version> is not provided, the latest available will be used to perform
the tests.

Usage:
  parallel-test [-N] [-W WORKERS] [-I INSTRUMENT] [-V VEREDICT [-v VERSION]] [-f FILELIST] [-M TEXT] [-m TEXT] [<version>]
  parallel-test (-h | --help)

Options:
  -h --help      Show this message
  -N             Don't skip files that have been tested already
  -W WORKERS     Number of parallel instances working on the data [default: 10]
  -I INSTRUMENT  Select only files for this instrument. Valid values are:
                    bhros, cirpass, flamingos, f2, gmos, gnirs, gpi, gsaoi,
                    hokuppa, hrwfs, michelle, nici, nifs, niri, oscir, phoenix,
                    quirc, texes, trecs
  -f FILELIST    Test files only from the filelist
  -M TEXT        Select cases where the 'cause' contains the indicated text
  -m TEXT        Select cases where the 'cause' DOES NOT contain the indicated text
  -V VEREDICT    Select only files which have been tested before and which came
                 up with this veredict in the last attempt. Valid values are:
                    bad, correct, eng, invalid, nodate, notgemini, exception
  -v VERSION     Specify the version of the test for which the veredict will be
                 taken into account. If not specified, the default is using the
                 same as for <version>
"""

from bz2 import BZ2File
from csv import reader
from docopt import docopt
from functools import partial
from multiprocessing import Process, Queue, JoinableQueue
from Queue import Empty as EmptyQueue
from ctypes import py_object
from collections import namedtuple
from os.path import join as opjoin, exists
from os import getpid
from pyfits import open as pfopen
from pyfits.verify import VerifyError
from astrodata import AstroData
from fits_storage.utils.gemini_fits_validator import RuleStack, RuleSet, Environment, AstroDataEvaluator
from fits_storage.utils.gemini_fits_validator import EngineeringImage, GeneralError, BadData, NotGeminiData, NoDateError
from io import BytesIO
from time import sleep

import psycopg2
import sys

# DSN and paths when running from hahalua
DSN = dict(dbname='fitsdata')
BASEPATH='/mnt/hahalua'
FIXEDBASEPATH='/mnt/hahalua/fixed/fixed_files'
#DSN = dict(host  ='rcardene-ld1',
#           dbname='fitsdata')
#BASEPATH='/data/gemini_data'
#FIXEDBASEPATH='/data/gemini_data/fixed/fixed_files'

def getConnection():
    return psycopg2.connect(**DSN)

def getDatabaseRulesetId(curs, version):
    if version is None:
        curs.execute("SELECT id, version FROM ruleset ORDER BY stamp DESC")
    else:
        curs.execute("SELECT id, version FROM ruleset WHERE ruleset.version = %s", (version,))
    try:
        return curs.fetchone()
    except TypeError:
        return None

class SqlRuleSet(RuleSet):
    file_info = {}
    def __init__(self, filename):
        super(SqlRuleSet, self).__init__(filename)

    def _open(self, filename):
        try:
            return BytesIO(self.__class__.file_info[filename])
        except (KeyError, TypeError):
            raise IOError("Can't find file '{0}' in database for ruleset version {1}".format(filename, _version.value))

def valid_header(rs, fits):
    fits.verify('exception')
    env = Environment()
    env.features = set()
    res = []
    mess = []
    for n, hdu in enumerate(fits):
        env.numHdu = n
        t = rs.test(hdu.header, env)
        res.append(t[0])
        mess.extend(t[1])
    return all(res), mess

def open_file(path):
    if path.lower().endswith('.bz2'):
        return pfopen(BZ2File(path))
    return pfopen(open(path))

Result = namedtuple('Result', ['filename', 'passes', 'code', 'messages'])

class Evaluator(AstroDataEvaluator):
    def __init__(self, *args, **kw):
        super(Evaluator, self).__init__(*args, **kw)

    def evaluate(self, filename):
        if filename.endswith('.bz2'):
            bz2n  = filename
            nobz2 = filename[:-4]
        else:
            bz2n  = filename + '.bz2'
            nobz2 = filename
        origpath = opjoin(BASEPATH, bz2n)
        fixedpath = opjoin(FIXEDBASEPATH, nobz2)
        fixedpathbz2 = opjoin(FIXEDBASEPATH, bz2n)
        if exists(fixedpathbz2):
            origpath = fixedpathbz2
            filename = bz2n
        elif exists(fixedpath):
            origpath = fixedpath
            filename = nobz2
        try:
            result = partial(Result, nobz2)
            ad_object = AstroData(open_file(origpath))
            return result(*super(Evaluator, self).evaluate(ad_object))
        except (GeneralError, IOError, VerifyError) as e:
            return result(False, 'EXCEPTION', e)

    def __call__(self, filename):
        return self.evaluate(filename)

get_filelist_query = "SELECT id, filename FROM testing_file AS tf"
filelist_join = "INNER JOIN testing_info as ti ON tf.id = ti.tested_file"
skip_query = """
 WHERE NOT EXISTS (SELECT NULL FROM testing_info AS tis
                    WHERE tis.test_version = %(vers)s
                      AND tis.tested_file = tf.id)
"""
instrument_filter = "tf.instrument = %(inst)s"
instrument_filter_in = "tf.instrument IN %(inst)s"
veredict_filter = "ti.veredict = %(vered)s AND ti.test_version = %(vered_vers)s"
cause_filter = "ti.causes LIKE %(causes)s"
notcause_filter = "ti.causes NOT LIKE %(notcauses)s"
file_filter = "tf.filename IN %(filelist)s"

insert_test_query = """
INSERT INTO testing_info (tested_file, test_version, acceptable, veredict, causes)
                   VALUES(%(tf)s, %(tv)s, %(acc)s, %(code)s, %(cau)s)
"""

update_test_query = """
UPDATE testing_info SET acceptable = %(acc)s, veredict = %(code)s, causes = %(cau)s, stamp = NOW()
 WHERE tested_file = %(tf)s AND test_version = %(tv)s
"""

def formatted_statement(statement, reindent=True):
    import sqlparse

    return sqlparse.format(statement, reindent=reindent)

class SqlFiles(object):
    def __init__(self, versid, skipping = True, filter=None):
        self.__conn = None
        self.versid = versid
        self.skipping = skipping
        self.filter = filter

    @property
    def conn(self):
        if self.__conn is None:
            self.__conn = getConnection()
        return self.__conn

    @property
    def query(self):
        extra_where = []
        arguments = { 'vers': self.versid }
        build_query = [ get_filelist_query ]
        if self.filter:
            if 'instrument' in self.filter:
                inst = self.filter['instrument']
                if isinstance(inst, (tuple, list)):
                    extra_where.append(instrument_filter_in)
                else:
                    extra_where.append(instrument_filter)
                arguments['inst'] = inst
            if 'filelist' in self.filter:
                extra_where.append(file_filter)
                arguments['filelist'] = tuple("{0}".format(x.strip()) for x in self.filter['filelist'])
            if 'veredict' in self.filter:
                build_query.append(filelist_join)
                extra_where.append(veredict_filter)
                arguments['vered'] = self.filter['veredict']
                arguments['vered_vers'] = self.filter.get('veredict-version', self.versid)
            if 'causes' in self.filter:
                extra_where.append(cause_filter)
                arguments['causes'] = '%{0}%'.format(self.filter['causes'])
            if 'notcauses' in self.filter:
                extra_where.append(notcause_filter)
                arguments['notcauses'] = '%{0}%'.format(self.filter['notcauses'])

        if self.skipping:
            build_query.append(skip_query)

        query = '\n'.join(build_query)
        if extra_where:
            filter_string = ' AND '.join(extra_where)
            if 'WHERE' in query:
                query = ' AND '.join((query, filter_string))
            else:
                query = ' WHERE '.join((query, filter_string))

        # print(formatted_statement(query))

        return query, arguments

    def __iter__(self):
        curs = self.conn.cursor()
        curs.execute(*self.query)

        for n, (fid, fname) in enumerate(curs, 1):
            yield fid, fname.strip()

    def __call__(self, output_queue):
        try:
            for candidate in self:
                output_queue.put(candidate)
            sleep(2)
            output_queue.join()
        except KeyboardInterrupt:
            output_queue.close()

class Upserter(object):
    def __init__(self, conn, versid):
        self.conn = conn
        self.versid = versid

    def __call__(self, fileid, result):
        curs = self.conn.cursor()

        data = dict(tf = fileid,
                    tv = self.versid,
                    acc = result.passes,
                    code = result.code,
                    cau = None)
        if result.messages is not None:
            if isinstance(result.messages, (tuple, list)):
                data['cau'] = '\n'.join(m for m in result.messages if isinstance(m, (str, unicode)))
            else:
                data['cau'] = str(result.messages)

        try:
            curs.execute(insert_test_query, data)
        except psycopg2.IntegrityError:
            self.conn.rollback()
            curs.execute(update_test_query, data)
        self.conn.commit()

instruments = {
    'bhros': 'BHROS',
    'cirpass': 'CIRPASS',
    'flamingos': 'FLAMINGOS',
    'f2': ('F2', 'FLAM'),
    'gmos': ('GMOS', 'GMOS-N', 'GMOS-S'),
    'gnirs': 'GNIRS',
    'gpi': 'GPI',
    'gsaoi': 'GSAOI',
    'hokuppa': 'HOKUPPA+QUIRCS',
    'hrwfs': 'HRWFS',
    'michelle': 'MICHELLE',
    'nici': 'NICI',
    'nifs': 'NIFS',
    'niri': 'NIRI',
    'oscir': 'OSCIR',
    'phoenix': 'PHOENIX',
    'quirc': 'QUIRC',
    'texes': 'TEXES',
    'trecs': 'TRECS',
    }

veredicts = {
    'bad': 'BAD',
    'correct': 'CORRECT',
    'eng': 'ENG',
    'invalid': 'NOPASS',
    'nodate': 'NODATE',
    'notgemini': 'NOTGEMINI',
    'exception': 'EXCEPTION',
    }

def getFilter(curs, args):
    filt = {}
    if args['-I']:
        try:
            filt['instrument'] = instruments[args['-I'].lower()]
        except KeyError:
            raise RuntimeError("'{0}' is not a valid instrument choice".format(args['-I']))
    if args['-V']:
        try:
            filt['veredict'] = veredicts[args['-V'].lower()]
            if args['-v']:
                filt['veredict-version'] = getDatabaseRulesetId(curs, args['-v'])[0]
        except KeyError:
            raise RuntimeError("'{0}' is not a valid veredict choice".format(args['-V']))
    if args['-M']:
        filt['causes'] = args['-M']

    if args['-m']:
        filt['notcauses'] = args['-m']

    if args['-f']:
        try:
            filt['filelist'] = open(args['-f'])
        except IOError:
            raise RuntimeError("Can't open file '{0}'".format(args['-f']))

    return filt

get_fileinfo_query = """
SELECT path, content
  FROM ruleset INNER JOIN ruleset_rulefile ON ruleset.id = ruleset_rulefile.set_id
               INNER JOIN rulefile ON ruleset_rulefile.file_id = rulefile.id
 WHERE ruleset.version = %s
"""

def getFileInfo(curs, vers):
    curs.execute(get_fileinfo_query, (vers,))

    return dict(curs)

def evaluatorProcess((versid, version), input_queue, output_queue):
    try:
        with getConnection() as conn:
            upsert = Upserter(conn, versid)
            SqlRuleSet.file_info = getFileInfo(conn.cursor(), version)
            evaluator = Evaluator(SqlRuleSet)
            while True:
                try:
                    (fileid, fname) = input_queue.get(timeout=2)
                except EmptyQueue:
                    continue
                input_queue.task_done()
                result = evaluator(fname)
                upsert(fileid, result)
                output_queue.put(result)
    except KeyboardInterrupt:
        output_queue.close()

class Pool(object):
    def __init__(self, num_processes):
        self.nprocs = num_processes

    def map(self, eval_func, args, sentinel):
        result_queue = JoinableQueue(CHUNK_SIZE)
        procs = [Process(target=eval_func, args=args + (result_queue,)) for n in range(self.nprocs)]
        for p in procs:
            p.start()

        while True:
            try:
                yield result_queue.get(timeout=2)
                result_queue.task_done()
            except EmptyQueue:
                if not sentinel.is_alive():
                    break

        result_queue.join()
        for p in procs:
            p.terminate()
            p.join()

if __name__ == '__main__':
    args = docopt(__doc__, version='Parallel Test v0.1')
    p = Pool(int(args['-W']))
    skip = not args['-N']
    CHUNK_SIZE = 64
    source_queue = JoinableQueue(CHUNK_SIZE)

    fileGen = None
    try:
        with getConnection() as conn:
            versid, vers = getDatabaseRulesetId(conn.cursor(), args['<version>'])
            # _version.value = vers
            print("Collecting rule set version {0}".format(vers), file=sys.stderr)
            sqlFiles = SqlFiles(versid, skipping=skip, filter=getFilter(conn.cursor(), args))
            fileGen = Process(target=sqlFiles, args=(source_queue,))
            fileGen.start()

            for result in p.map(evaluatorProcess, ((versid, vers), source_queue), sentinel = fileGen):
                print("{0:40} - {1}/{2}".format(result.filename, result.passes, result.code))

            fileGen.join()
    except RuntimeError as e:
        print(e, file=sys.stderr)
        sys.exit(-1)
    except KeyboardInterrupt:
        print("\nInterrupted - leaving children some time to die")
        sleep(4)
