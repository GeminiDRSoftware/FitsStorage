import os.path
import random
import logging

from fits_storage.server.aws_s3 import Boto3Helper
from fits_storage.config import get_config
from fits_storage.core.hashes import md5sum

fsc = get_config()


def test_aws_s3(tmp_path):
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
                     logger=logging.getLogger())

    assert not bh.exists_key(filename)

    assert bh.upload_file(filename, fpfn) is not None

    assert bh.exists_key(filename)
    assert bh.get_md5(filename) == md5

    assert bh.fetch_to_storageroot(filename)

    srfpfn = os.path.join(storageroot, filename)
    assert os.path.exists(srfpfn)
    srmd5 = md5sum(srfpfn)
    assert srmd5 == md5

    assert bh.delete_key(filename)

    assert not bh.exists_key(filename)