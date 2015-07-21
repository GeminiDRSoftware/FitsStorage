import urllib
import datetime

xml="<qareport> <hostname>myhost.gemini.edu</hostname> <userid>phirst</userid> <processid>10245</processid> <executable>reduce</executable> <software>QAP</software> <software_version>1.30</software_version> <context>QA</context> <qametric> <datalabel>GN-2012A-Q-47-55-004</datalabel> <filename>N20120203S0123_forStack.fits</filename> <detector>EEV 23451-1, left</detector> <iq> <fwhm>0.735</fwhm> <fwhm_std>0.123</fwhm_std> <isofwhm>0.825</isofwhm> <isofwhm_std>0.113</isofwhm_std> <ee50d>0.935</ee50d> <ee50d_std>0.02</ee50d_std> <elip>0.002</elip> <elip_std>0.001</elip_std> <pa>156.3</pa> <pa_std>30.4</pa_std> <nsamples>42</nsamples> <percentile_band>IQ70</percentile_band> <comment>faint guide star</comment> </iq> <zp> <mag>27.432</mag> <mag_std>0.133</mag_std> <cloud>0.23</cloud> <cloud_std>0.05</cloud_std> <photref>SDSS7</photref> <nsamples>923</nsamples> <percentile_band>CC80</percentile_band> <comment>globular cluster</comment> </zp> <sb> <mag>22.34</mag> <mag_std>0.34</mag_std> <electrons>4256.2</electrons> <electrons_std>123.4</electrons_std> <nsamples>2342</nsamples> <percentile_band>BGAny</percentile_band> <comment>car headlights?</comment> </sb> <pe> <dra>1.24</dra> <dra_std>0.03</dra_std> <ddec>0.23</ddec> <ddec_std>0.02</ddec_std> <astref>SDSS7</astref> <nsamples>23</nsamples> <comment>astrometric field</comment> </pe> </qametric> </qareport>"

postdata = urllib.quote(xml)

url = "http://hbffits2/qareport"
u = urllib.urlopen(url, postdata)

print u.read()
u.close()
print u.getcode()
