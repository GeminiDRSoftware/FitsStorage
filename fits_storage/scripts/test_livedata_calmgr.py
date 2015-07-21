import urllib
import datetime
from astrodata import AstroData

# This is a GMOS_N imaging science dataset
ad = AstroData("/net/wikiwiki/dataflow/N20121104S0066.fits")


desc_dict = {'instrument':ad.instrument().for_db(),
             'disperser':ad.disperser().for_db(),
             'central_wavelength':ad.central_wavelength(asMicrometers=True).for_db(),
             'observation_type':ad.observation_type().for_db(),
             'data_label':ad.data_label().for_db(),
             'detector_x_bin':ad.detector_x_bin().for_db(),
             'detector_y_bin':ad.detector_y_bin().for_db(),
             'read_speed_setting':ad.read_speed_setting().for_db(),
             'gain_setting':ad.gain_setting().for_db(),
             'amp_read_area':ad.amp_read_area().for_db(),
             'ut_datetime':ad.ut_datetime().for_db(),
             'exposure_time':ad.exposure_time().for_db(),
             'object':ad.object().for_db(),
             'filter_name':ad.filter_name().for_db(),
             'focal_plane_mask':ad.focal_plane_mask().for_db(),
             }
type_list = ad.types
ad.close()

start = datetime.datetime.now()
sequence = (('descriptors', desc_dict), ('types', type_list))
postdata = urllib.urlencode(sequence)

#print desc_dict


url = "http://fits/calmgr/processed_flat/"
u = urllib.urlopen(url, postdata)
end = datetime.datetime.now()

interval = end - start
print u.read()
u.close()

print "-----\n"
print "query took %s" % interval
