import urllib
import datetime

xml = "<qareport> <hostname>myhost.gemini.edu</hostname> <userid>phirst</userid> <processid>10245</processid> <executable>reduce</executable> <software>QAP</software> <software_version>1.30</software_version> <context>QA</context> <qametric> <datalabel>GN-CAL20120306-7-008</datalabel> <filename>N20120203S0123_forStack.fits</filename> <iq> <fwhm>0.735</fwhm> <fwhmerr>0.123</fwhmerr> <elip>0.002</elip> <eliperr>0.001</eliperr> <pa>156.3</pa> <paerr>30.4</paerr> </iq> <zp> <mag>27.432</mag> <magerr>0.133</magerr> </zp> <sb> <mag>22.34</mag> <magerr>0.34</magerr> </sb> <pe> <dra>1.24</dra> <draerr>0.03</draerr> <ddec>0.23</ddec> <ddecerr>0.02</ddecerr> </pe> </qametric> </qareport>"

postdata = urllib.quote(xml)

url = "http://hbffits2/qareport"
u = urllib.urlopen(url, postdata)

print u.read()
u.close()
print u.getcode()
