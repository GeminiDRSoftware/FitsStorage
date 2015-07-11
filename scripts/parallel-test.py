#!/usr/bin/env python

from __future__ import print_function

__doc__ = """Parallel Databased Test

If <version> is not provided, the latest available will be used to perform
the tests.

Usage:
  parallel-test [-W WORKERS] [-I INSTRUMENT] [-V VEREDICT [-v VERSION]] [<version>]
  parallel-test (-h | --help)

Options:
  -h --help      Show this message
  -W WORKERS     Number of parallel instances working on the data [default: 10]
  -I INSTRUMENT  Select only files for this instrument. Valid values are:
                    bhros, flamingos, f2, gmos, gnirs, gpi, gsaoi, hokuppa,
                    hrwfs, michelle, nici, nifs, niri, oscir, phoenix,
                    quirc, texes, trecs
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
from pathos.multiprocessing import Pool
from collections import namedtuple
from os.path import join as opjoin, exists
from pyfits import open as pfopen
from pyfits.verify import VerifyError
from astrodata import AstroData
from utils.fits_validator import RuleStack, RuleSet, Environment, AstroDataEvaluator
from utils.fits_validator import EngineeringImage, GeneralError, BadData, NotGeminiData, NoDateError
from io import BytesIO

import psycopg2
import sys

# DSN and paths when running from hahalua
#DSN = dict(host  ='rcardene-ld1',
#           dbname='fitsdata')
DSN={'dbname': 'fitsdata'}

#BASEPATH='/data/gemini_data'
#FIXEDBASEPATH='/data/gemini_data/fixed/fixed_files'
BASEPATH='/mnt/hahalua'
FIXEDBASEPATH='/mng/hahalua/fixed/fixed_files'

def getDatabaseRulesetId(curs, version):
    if version is None:
        curs.execute("SELECT id, version FROM ruleset ORDER BY stamp DESC")
    else:
        curs.execute("SELECT id, version FROM ruleset WHERE ruleset.version = %s", (version,))
    try:
        return curs.fetchone()
    except TypeError:
        return None

get_ids_query = """
SELECT path, rulefile.id
  FROM ruleset INNER JOIN ruleset_rulefile ON ruleset.id = ruleset_rulefile.set_id
               INNER JOIN rulefile ON ruleset_rulefile.file_id = rulefile.id
 WHERE ruleset.version = %s
"""

get_document_query = "SELECT content FROM rulefile WHERE rulefile.id = %s"

class SqlRuleSet(RuleSet):
    __conn = None
    __version = None
    __file_ids = {}

    @classmethod
    def initializeSql(cls, conn, ver):
        cls.__conn = conn
        cls.__version = ver
        curs = conn.cursor()
        curs.execute(get_ids_query, (ver,))
        cls.__file_ids = dict(curs.fetchall())

    @classmethod
    def _open(cls, filename):
        try:
            curs = cls.__conn.cursor()
            curs.execute(get_document_query, (cls.__file_ids[filename],))

            return BytesIO(curs.fetchone()[0])
        except (KeyError, TypeError):
            raise IOError("Can't find file '{0}' in database for ruleset version {1}".format(filename, cls.__version))

    def __init__(self, filename):
        super(SqlRuleSet, self).__init__(filename)

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
        if exists(fixedpath):
            origpath = fixedpath
            filename = nobz2
        try:
            result = partial(Result, nobz2)
            ad_object = AstroData(open_file(origpath))
            return result(*super(Evaluator, self).evaluate(ad_object))
#            try:
#                valid, msg = valid_header(self.rs, open_file(origpath))
#                if valid:
#                    return result(True, 'CORRECT', None)
#                else:
#                    return result(False, 'NOPASS', msg)
#            except NoDateError:
#                return result(False, 'NODATE', None)
#            except NotGeminiData:
#                return result(False, 'NOTGEMINI', None)
#            except BadData:
#                return result(False,  'BAD', None)
#            except EngineeringImage:
#                return result(True, 'ENG', None)
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
    def __init__(self, conn, versid, skipping = True, filter=None):
        self.conn = conn
        self.versid = versid
        self.skipping = skipping
        self.collected = {}
        self.filter = filter

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
            if 'veredict' in self.filter:
                build_query.append(filelist_join)
                extra_where.append(veredict_filter)
                arguments['vered'] = self.filter['veredict']
                arguments['vered_vers'] = self.filter.get('veredict-version', self.versid)

        if self.skipping:
            build_query.append(skip_query)

        query = '\n'.join(build_query)
        if extra_where:
            filter_string = ' AND '.join(extra_where)
            if 'WHERE' in query:
                query = ' AND '.join((query, filter_string))
            else:
                query = ' WHERE '.join((query, filter_string))

        return query, arguments

    def __iter__(self):
        curs = self.conn.cursor()
        curs.execute(*self.query)

        for fid, fname in curs:
            self.collected[fname] = fid
            yield fname.strip()

    def upsert(self, result):
        curs = self.conn.cursor()

        fileid = self.collected[result.filename]
        del self.collected[result.filename]

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

    return filt

if __name__ == '__main__':
    args = docopt(__doc__, version='Parallel Test v0.1')
    p = Pool(int(args['-W']))

    try:
        with psycopg2.connect(**DSN) as conn:
            versid, vers = getDatabaseRulesetId(conn.cursor(), args['<version>'])
            print("Collecting rule set version {0}".format(vers), file=sys.stderr)
            SqlRuleSet.initializeSql(conn, vers)
            evaluator = Evaluator(SqlRuleSet)
            sqlFiles = SqlFiles(conn, versid, filter=getFilter(conn.cursor(), args))

            for result in p.imap_unordered(evaluator, sqlFiles, chunksize = 10):
                print("{0:40} - {1}/{2}".format(result.filename, result.passes, result.code))
                sqlFiles.upsert(result)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        sys.exit(-1)
