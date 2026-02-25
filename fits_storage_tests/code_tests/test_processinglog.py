import time
from sqlalchemy import select

from fits_storage_tests.code_tests.helpers import make_empty_testing_db_env

from fits_storage.server.orm.processinglog import ProcessingLog, ProcessingLogFile

class DummyRQE(object):
    def __init__(self, tag=None, filenames=[], recipe=None, debundle=None,
                 capture_files=False, capture_monitoring=False):
        self.tag = tag
        self.filenames = filenames
        self.recipe = recipe
        self.debundle = debundle
        self.capture_files = capture_files
        self.capture_monitoring = capture_monitoring

def test_processinglog_sanity():
    rqe = DummyRQE(tag='testtag', filenames=['file1.fits', 'file2.fits'],
                   recipe='testrecipe', debundle=False,
                   capture_files=True, capture_monitoring=True)
    p = ProcessingLog(rqe)

    assert p.processing_tag == 'testtag'
    assert p.input_files == 'file1.fits, file2.fits'
    assert p.num_input_files == 2
    assert p.debundle == False
    assert p.recipe == 'testrecipe'

    reduced_files = ['foo/out1.fits', 'out2.fits']
    time.sleep(1)
    p.end(reduced_files, failed=False)

    assert p.failed is False
    assert p.output_files == 'out1.fits, out2.fits'
    assert p.num_output_files == 2
    assert p.cpu_secs > 0
    assert p.cpu_secs < 1

def test_processinglogfile_sanity():
    plf = ProcessingLogFile(1, filename='file1.file', md5sum='12345', output=False)
    assert plf.processinglog_id == 1
    assert plf.filename == 'file1.file'
    assert plf.md5sum == '12345'
    assert plf.output is False

def test_processinglog(tmp_path):
    rqe = DummyRQE(tag='testtag', filenames=['file1.fits', 'file2.fits'],
                   recipe='testrecipe', debundle=None,
                   capture_files=True, capture_monitoring=True)

    session = make_empty_testing_db_env(tmp_path)

    pl = ProcessingLog(rqe)
    assert pl.input_files == 'file1.fits, file2.fits'

    session.add(pl)
    session.commit()

    plf1 = ProcessingLogFile(pl.id, filename='file1.fits', md5sum='12345', output=False)
    plf2 = ProcessingLogFile(pl.id, filename='file2.fits', md5sum='abcde', output=False)
    session.add(plf1)
    session.add(plf2)

    session.commit()

    stmt = (select(ProcessingLogFile)
            .where(ProcessingLogFile.filename=='file1.fits')
            .where(ProcessingLogFile.md5sum=='12345')
            .where(ProcessingLogFile.output==False))
    plfs = session.execute(stmt).scalars().all()

    assert plfs[0].filename == 'file1.fits'
    assert plfs[0].processinglog_id == pl.id
    assert plfs[0].processinglog.id == 1
    assert plfs[0].processinglog.input_files == 'file1.fits, file2.fits'
