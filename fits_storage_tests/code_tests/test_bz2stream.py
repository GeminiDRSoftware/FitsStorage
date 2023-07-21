import random
import io
import bz2
import hashlib

from fits_storage.server.bz2stream import StreamBz2Compressor

# Note random.randbytes(n) returns n random bytes
# Note io.BytesIO(initial_bytes=foo) gives a file-like-object containing foo

def do_test(datasize=1, chunksize=1, readsize=1):
    src_data = random.randbytes(datasize)
    check_data = bz2.compress(src_data)
    hashobj = hashlib.md5()
    hashobj.update(check_data)

    src_flo = io.BytesIO(initial_bytes=src_data)
    comp_data = bytes(0)

    sbc = StreamBz2Compressor(src_flo, chunksize=chunksize)

    while data := sbc.read(readsize):
        comp_data += data

    assert comp_data == check_data
    assert sbc.bytes_output == len(check_data)
    assert sbc.md5sum_output == hashobj.hexdigest()

# Walk through combinations of sizes being bigger or smaller than eachother

def test_bzstream1():
    do_test(datasize=100, chunksize=10, readsize=1)

def test_bzstream2():
    do_test(datasize=100, chunksize=10, readsize=200)

def test_bzstream3():
    do_test(datasize=100, chunksize=1000, readsize=1)

def test_bzstream4():
    do_test(datasize=100, chunksize=1000, readsize=200)

def test_bzstream5():
    do_test(datasize=5000000, chunksize=1000000, readsize=1000)

