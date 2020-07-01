import urllib.request, urllib.parse, urllib.error
import datetime


if __name__ == "__main__":
    json=b'[{"hostname":"myhost.gemini.edu", "userid":"phirst", "processid":10245, "executable":"reduce", "software":"QAP", "software_version":"1.30", "context":"QA", "qametric":[{"datalabel":"GN-2012A-Q-47-55-004", "filename":"Z19990203S0123_forStack.fits", "detector":"EEV 23451-1, left", "iq":{"fwhm":0.735, "fwhm_std":0.123, "isofwhm":0.825, "isofwhm_std":0.113, "ee50d":0.935, "ee50d_std":0.02, "elip":0.002, "elip_std":0.001, "pa":156.3, "pa_std":30.4, "nsamples":42 ,"percentile_band":"IQ70", "adaptive_optics":"True", "ao_seeing":0.75, "comment":["faint guide star"]}, "zp":{"mag":27.432, "mag_std":0.133, "cloud":0.23, "cloud_std":0.05, "photref":"SDSS7", "nsamples":923, "percentile_band":"CC80", "comment":["globular cluster"]}, "sb":{"mag":22.34, "mag_std":0.34, "electrons":4256.2, "electrons_std":123.4, "nsamples":2342, "percentile_band":"BGAny", "comment":["car headlights?"]}, "pe":{"dra":1.24, "dra_std":0.03, "ddec":0.23, "ddec_std":0.02, "astref":"SDSS7", "nsamples":23, "comment":["astrometric field"]}}]}]'

    url = "http://mkofits-lv1/qareport"
    u = urllib.request.urlopen(url, json)

    print(u.read())
    u.close()
    print(u.getcode())
