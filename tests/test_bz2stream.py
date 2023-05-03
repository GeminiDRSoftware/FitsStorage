import random
import io
import bz2

from fits_storage.server.bz2stream import StreamBz2Compressor

# Note random.randbytes(n) returns n random bytes
# Note io.BytesIO(initial_bytes=foo) gives a file-like-object containing foo

def do_test(datasize=1, chunksize=1, readsize=1):
    src_data = random.randbytes(datasize)
    check_data = bz2.compress(src_data)

    src_flo = io.BytesIO(initial_bytes=src_data)
    comp_data = bytes(0)

    sbc = StreamBz2Compressor(src_flo, chunksize=chunksize)

    while data := sbc.read(readsize):
        comp_data += data

    assert comp_data == check_data

# Walk through combinations of sizes being bigger or smaller than eachother

def test_1():
    do_test(datasize=100, chunksize=10, readsize=1)

def test_2():
    do_test(datasize=100, chunksize=10, readsize=200)

def test_3():
    do_test(datasize=100, chunksize=1000, readsize=1)

def test_4():
    do_test(datasize=100, chunksize=1000, readsize=200)

def test_5():
    do_test(datasize=5000000, chunksize=1000000, readsize=1000)
