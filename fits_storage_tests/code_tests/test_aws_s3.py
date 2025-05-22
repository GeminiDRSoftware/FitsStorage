import pytest
import os.path
import random

from fits_storage.server.aws_s3 import Boto3Helper
from fits_storage.config import get_config
from fits_storage.core.hashes import md5sum, md5sum_size_fp

from fits_storage.logger_dummy import DummyLogger

fsc = get_config(builtinonly=True, reload=True)
# Need to pull in test bucket keys for these to run.
# fsc = get_config()
logger = DummyLogger(print=True)

@pytest.mark.skip()
@pytest.mark.skipif(fsc.testing_aws_access_key == '',
                    reason='Current config does not provide test aws keys')
def test_aws_s3_file(tmp_path):
    s3staging = os.path.join(tmp_path, 's3_staging')
    storageroot = os.path.join(tmp_path, 'storage_root')
    os.mkdir(s3staging)
    os.mkdir(storageroot)

    # Make a 1MB file of random data in s3staging
    filename = 'testfile.dat'
    fpfn = os.path.join(s3staging, filename)
    with open(fpfn, 'wb') as f:
        f.write(random.randbytes(1000000))
    md5 = md5sum(fpfn)

    bh = Boto3Helper(bucket_name='gemini-archive-test',
                     access_key=fsc.testing_aws_access_key,
                     secret_key=fsc.testing_aws_secret_key,
                     s3_staging_dir=s3staging,
                     storage_root=storageroot,
                     logger=logger)

    assert not bh.exists_key(filename)

    assert bh.upload_file(filename, fpfn) is not None

    assert bh.exists_key(filename)
    assert bh.get_md5(filename) == md5

    assert bh.fetch_to_storageroot(filename)

    srfpfn = os.path.join(storageroot, filename)
    assert os.path.exists(srfpfn)
    srmd5 = md5sum(srfpfn)
    assert srmd5 == md5

    newfilename = "testrename.dat"
    bh.rename(filename, newfilename)

    assert bh.exists_key(newfilename)
    assert not bh.exists_key(filename)
    assert bh.get_md5(newfilename) == md5

    assert bh.delete_key(newfilename)

    assert not bh.exists_key(newfilename)

@pytest.mark.skip()
@pytest.mark.skipif(fsc.testing_aws_access_key == '',
                    reason='Current config does not provide test aws keys')
def test_s3_copy():
    # Test copy between buckets. This is used for the glacier backup.
    # Fundamentally, S3 has 2 types of copy - regular copy_object is limited
    # to objects less than 5GB. Bigger than that you have to do a multipart
    # copy. In the past you had to do this yourself, but now the Boto3 module
    # can handle that for you. We test both here (a miscfile, 7.7GB, > 5GB) and
    # a small (N2021... 2.9MB) file, that are stored in the
    # gemini-archive-test-1 bucket.

    large_file = 'miscfile_2012.07.27.tar'
    large_file_md5 = '557f2afaa2b55ff34a020fa5acc855af'

    small_file = 'N20120101S0001.fits.bz2'
    small_file_md5 = 'f645412d58332995bf7f0dce808933f4'

    # First ensure that the test files don't already exist in the -test bucket
    # Helper for the destination bucket
    bh = Boto3Helper(bucket_name='gemini-archive-test',
                     access_key=fsc.testing_aws_access_key,
                     secret_key=fsc.testing_aws_secret_key,
                     logger=logger)

    if bh.exists_key(large_file):
        bh.delete_key(large_file)
    assert not bh.exists_key(large_file)

    if bh.exists_key(small_file):
        bh.delete_key(small_file)
    assert not bh.exists_key(small_file)

    # Helper for the source bucket
    bh1 = Boto3Helper(bucket_name='gemini-archive-test-1',
                      access_key=fsc.testing_aws_access_key,
                      secret_key=fsc.testing_aws_secret_key,
                      logger=logger)

    # Check md5sums are as expected
    assert bh1.get_md5(small_file) == small_file_md5
    assert bh1.get_md5(large_file) == large_file_md5

    # Copy the files
    bh1.copy(small_file, 'gemini-archive-test')
    assert bh.exists_key(small_file)

    bh1.copy(large_file, 'gemini-archive-test')
    assert bh.exists_key(large_file)

    # Check md5 metadata got copied
    assert bh.get_md5(small_file) == small_file_md5
    assert bh.get_md5(large_file) == large_file_md5

    # Tidy up
    bh.delete_key(small_file)
    bh.delete_key(large_file)

@pytest.mark.skip()
def test_aws_s3_get_flo(tmp_path):
    s3staging = os.path.join(tmp_path, 's3_staging')
    storageroot = os.path.join(tmp_path, 'storage_root')
    os.mkdir(s3staging)
    os.mkdir(storageroot)

    # Make a 1MB file of random data in s3staging
    filename = 'testfile.dat'
    fpfn = os.path.join(s3staging, filename)
    with open(fpfn, 'wb') as f:
        f.write(random.randbytes(1000000))
    md5 = md5sum(fpfn)

    bh = Boto3Helper(bucket_name='gemini-archive-test',
                     access_key=fsc.testing_aws_access_key,
                     secret_key=fsc.testing_aws_secret_key,
                     s3_staging_dir=s3staging,
                     storage_root=storageroot,
                     logger=logger)

    assert not bh.exists_key(filename)

    assert bh.upload_file(filename, fpfn) is not None

    assert bh.exists_key(filename)
    assert bh.get_md5(filename) == md5

    flo = bh.get_flo('testfile.dat')

    sizeflo, md5flo = md5sum_size_fp(flo)

    assert sizeflo == 1000000
    assert md5flo == md5

    flo.close()

    assert bh.delete_key(filename)
    assert not bh.exists_key(filename)

def test_underlaylay(tmp_path):
    s3staging = os.path.join(tmp_path, 's3_staging')
    storageroot = os.path.join(tmp_path, 'storage_root')
    os.mkdir(s3staging)
    os.mkdir(storageroot)

    small_file = 'N20120101S0001.fits.bz2'
    small_file_md5 = 'f645412d58332995bf7f0dce808933f4'


    # Make a 1kB file of random data in s3staging
    filename = 'testfile.dat'
    fpfn = os.path.join(s3staging, filename)
    with open(fpfn, 'wb') as f:
        f.write(random.randbytes(1000))
    testmd5 = md5sum(fpfn)

    # Create a helper on -test *without* the underlay bucket
    bh = Boto3Helper(bucket_name='gemini-archive-test',
                     access_key=fsc.testing_aws_access_key,
                     secret_key=fsc.testing_aws_secret_key,
                     s3_staging_dir=s3staging,
                     storage_root=storageroot,
                     logger=logger)

    # Check that small_file does not exist here
    if bh.exists_key(small_file):
        bh.delete_key(small_file)
    assert not bh.exists_key(small_file)

    # Upload the random data file
    if bh.exists_key(filename):
        bh.delete_key(filename)
    assert not bh.exists_key(filename)
    assert bh.upload_file(filename, fpfn) is not None
    assert bh.exists_key(filename)

    # Create a helper on the underlay bucket
    bh = Boto3Helper(bucket_name='gemini-archive-test-1',
                     access_key=fsc.testing_aws_access_key,
                     secret_key=fsc.testing_aws_secret_key,
                     s3_staging_dir=s3staging,
                     storage_root=storageroot,
                     logger=logger)

    # Check that small_file exists on the underlay.
    assert bh.exists_key(small_file)

    # Now create a helper on the test bucket *with* the underlay bucket
    bh = Boto3Helper(bucket_name='gemini-archive-test',
                     underlay_bucket_name='gemini-archive-test-1',
                     access_key=fsc.testing_aws_access_key,
                     secret_key=fsc.testing_aws_secret_key,
                     s3_staging_dir=s3staging,
                     storage_root=storageroot,
                     logger=logger)

    # Verify that small_file exists and can access it
    assert bh.exists_key(small_file)
    assert bh.fetch_to_storageroot(small_file)
    fpfn = os.path.join(storageroot, small_file)
    assert os.path.exists(fpfn)
    srmd5 = md5sum(fpfn)
    assert srmd5 == small_file_md5

    # verify can access small_file as a flo
    assert bh.exists_key(small_file)
    flo = bh.get_flo(small_file)
    sizeflo, md5flo = md5sum_size_fp(flo)
    assert md5flo == small_file_md5
    flo.close()

    # Verify that the random data file exists and can access it
    assert bh.exists_key(filename)
    assert bh.fetch_to_storageroot(filename)
    fpfn = os.path.join(storageroot, filename)
    assert os.path.exists(fpfn)
    md5 = md5sum(fpfn)
    assert md5 == testmd5

    # verify can access the random data as a flo
    assert bh.exists_key(filename)
    flo = bh.get_flo(filename)
    sizeflo, md5flo = md5sum_size_fp(flo)
    assert sizeflo == 1000
    assert md5flo == testmd5
    flo.close()

    # clean up
    bh.delete_key(filename)
