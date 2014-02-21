import sys
sys.path.append("/opt/boto/lib/python2.6/site-packages/boto-2.23.0-py2.6.egg")

from boto.s3.connection import S3Connection
from boto.s3.key import Key

aws_access_key = 'AKIAJ554XPYMJZBPGQAA'
aws_secret_key = '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'

bucket_name = 'gemini-archive'

s3conn = S3Connection(aws_access_key, aws_secret_key)
bucket = s3conn.get_bucket(bucket_name)

k = Key(bucket)
k.key = 'foo.test'
k.set_contents_from_string('testing testing testing')

