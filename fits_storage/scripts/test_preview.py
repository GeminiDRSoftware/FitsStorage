import sys

import astrodata
import gemini_instruments

from fits_storage.utils.previewqueue import render_preview
from fits_storage.logger import logger, setdebug, setdemon


# Option Parsing
from optparse import OptionParser
parser = OptionParser()
parser.add_option("--file", action="store", type="string", dest="file",
                  help="file to preview")

parser.add_option("--debug", action="store_true", dest="debug",
                  help="Increase log level to debug")

parser.add_option("--demon", action="store_true", dest="demon",
                  help="Run as a background demon, do not generate stdout")

(options, args) = parser.parse_args()

# Logging level to debug? Include stdio log?
setdebug(options.debug)
setdemon(options.demon)

filename = options.file

if not filename:
    print("Must supply and input filename")
    sys.exit(1)

ad = astrodata.open(filename)
if not ad:
    print("AstroData open failed")
    sys.exit(2)

jpgfile = filename.replace('.fits', '.jpg')
fp = open(jpgfile, 'w')

print("input file: %s" % filename)
print("output file: %s" % jpgfile)

print("Rendering Preview...")

render_preview(ad, fp)

print("Done")

fp.close()
ad.close()

