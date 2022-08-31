import urllib.parse
import requests

import datetime
from requests import RequestException

import astrodata
import gemini_instruments


if __name__ == "__main__":

    # This is a GMOS_N imaging science dataset
    ad = astrodata.open("/Users/ooberdorf/Downloads/N20121104S0066.fits")
    desc_dict = {'instrument':ad.instrument(),
                 'disperser':ad.disperser(),
                 'central_wavelength':ad.central_wavelength(asMicrometers=True),
                 'observation_type':ad.observation_type(),
                 'data_label':ad.data_label(),
                 'detector_x_bin':ad.detector_x_bin(),
                 'detector_y_bin':ad.detector_y_bin(),
                 'read_speed_setting':ad.read_speed_setting(),
                 'gain_setting':ad.gain_setting(),
                 'amp_read_area':'+'.join(ad.amp_read_area()),
                 'ut_datetime':ad.ut_datetime(),
                 'exposure_time':ad.exposure_time(),
                 'object':ad.object(),
                 'filter_name':ad.filter_name(),
                 'focal_plane_mask':ad.focal_plane_mask(),
                 }

    # induce error
    del desc_dict['filter_name']

    type_list = ad.tags
    start = datetime.datetime.now()
    sequence = (('descriptors', desc_dict), ('types', type_list))
    postdata = urllib.parse.urlencode(sequence)

    #print desc_dict
    url = "http://localhost/calmgr/processed_flat/"
    try:
        r = requests.get(url, data=postdata)
        end = datetime.datetime.now()

        interval = end - start
        print(r.text)

        print("-----\n")
        print("query took %s" % interval)
    except RequestException as httpe:
        print("Got HTTP Error message: {}".format(httpe.readlines()))