
import sys
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

url = 'http://localhost:8090/miscfiles'

uploadFile = sys.argv[1]
uploadDesc = sys.argv[2]
program = sys.argv[3]

desc = ''
with open('foo.txt') as txt:
  desc = txt.read()

mp_encoder = MultipartEncoder(
  fields = {
    'upload': 'True',
    'uploadFile': (uploadFile, open(uploadFile, 'rb'), 'application/x-gzip'),
    'uploadRelease': 'now',
    'uploadProg': program,
    'uploadDesc': desc,
  }
)

cookies = {'gemini_archive_session': 'fAvMs11CedLRtzeXe0UNiLzEPUKcn01noLIkc38eTma2ESzzC4hTIET/6kUflKfV1/V9YuFn4Cs2qbaQRdQmjSUncdU+O3q8RRu/bgQyRC6/m73RK9qacr7y5NuKYU6pbJNgFycnAyFn/9d/fxajdxt/1mvIs6ZJtSE+1Dp7hhHHwESfVkJE0A9wNpQq2Rk8mKZUpe9jXDUoRQtHcyzND4qFir4Y1sBpECiE8IGgCCoNWTv5cbu06Xmb5FxDufSa26x5M2hOCHVxKVwSzD188MOBWxkPFBN0bVK55oahTMy3xTQaqLEgJtxM78di5H3rnHmeUS55Xbl7vMri1GKvow=='}

r = requests.post(url, data=mp_encoder, cookies=cookies, headers={'Content-Type': mp_encoder.content_type})
