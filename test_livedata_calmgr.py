import sys
sys.path.append("/opt/gemini_python")
from astrodata import AstroData
import urllib

ad = AstroData("/net/wikiwiki/dataflow/N20100820S0241.fits")


desc_dict = {'instrument':ad.instrument(),
             'detector_x_bin':ad.detector_x_bin(),
             'detector_y_bin':ad.detector_y_bin(),
             'read_speed_mode':ad.read_speed_mode(),
             'gain_mode':ad.gain_mode(),
             'amp_read_area':ad.amp_read_area(asList=True),
             'ut_date':ad.ut_date(),
             'ut_time':ad.ut_time(),
             }
type_list = ad.types

ad.close()

sequence = (('descriptors', desc_dict), ('types', type_list))

postdata = urllib.urlencode(sequence)

url = "http://hbffits1/calmgr/"

u = urllib.urlopen(url, postdata)

print u.read()

u.close()

