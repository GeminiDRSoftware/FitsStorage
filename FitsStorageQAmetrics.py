"""
This module contains the QA metric database interface
"""

import urllib
from mod_python import apache
from xml.dom.minidom import parseString
from FitsStorage import *

def qareport(req):
  """
  This function handles submnission of QA metric reports
  """
  session = sessionfactory()
  try:
    if(req.method == 'GET'):
      return apache.HTTP_NOT_ACCEPTABLE
 
    if(req.method == 'POST'):
      clientdata = req.read()
      clientstr = urllib.unquote(clientdata)
      req.log_error(clientstr)
      dom = parseString(clientstr)
      for qr in dom.getElementsByTagName("qareport"):
        qareport = QAreport()
        qareport.hostname=get_value(qr.getElementsByTagName("hostname"))
        qareport.userid=get_value(qr.getElementsByTagName("userid"))
        qareport.processid=get_value(qr.getElementsByTagName("processid"))
        qareport.executable=get_value(qr.getElementsByTagName("executable"))
        qareport.software=get_value(qr.getElementsByTagName("software"))
        qareport.software_version=get_value(qr.getElementsByTagName("software_version"))
        qareport.context=get_value(qr.getElementsByTagName("context"))
        qareport.submit_time = datetime.datetime.now()
        qareport.submit_host = req.get_remote_host(apache.REMOTE_NAME)
        session.add(qareport)
        session.commit()
        for qam in qr.getElementsByTagName("qametric"):
          datalabel = get_value(qam.getElementsByTagName("datalabel"))
          filename = get_value(qam.getElementsByTagName("filename"))
          for iq in qam.getElementsByTagName("iq"):
            qametriciq = QAmetricIQ(qareport)
            qametriciq.filename = filename
            qametriciq.datalabel = datalabel
            qametriciq.fwhm = get_value(iq.getElementsByTagName("fwhm"))
            qametriciq.fwhmerr = get_value(iq.getElementsByTagName("fwhmerr"))
            qametriciq.elip = get_value(iq.getElementsByTagName("elip"))
            qametriciq.eliperr = get_value(iq.getElementsByTagName("eliperr"))
            qametriciq.pa = get_value(iq.getElementsByTagName("pa"))
            qametriciq.paerr = get_value(iq.getElementsByTagName("paerr"))
            qametriciq.strehl = get_value(iq.getElementsByTagName("strehl"))
            qametriciq.strehlerr = get_value(iq.getElementsByTagName("strehlerr"))
            session.add(qametriciq)
            session.commit()
          for zp in qam.getElementsByTagName("zp"):
            qametriczp = QAmetricZP(qareport)
            qametriczp.filename = filename
            qametriczp.datalabel = datalabel
            qametriczp.mag = get_value(zp.getElementsByTagName("mag"))
            qametriczp.magerr = get_value(zp.getElementsByTagName("magerr"))
            session.add(qametriczp)
            session.commit()
          for sb in qam.getElementsByTagName("sb"):
            qametricsb = QAmetricSB(qareport)
            qametricsb.filename = filename
            qametricsb.datalabel = datalabel
            qametricsb.mag = get_value(sb.getElementsByTagName("mag"))
            qametricsb.magerr = get_value(sb.getElementsByTagName("magerr"))
            session.add(qametricsb)
            session.commit()
          for pe in qam.getElementsByTagName("pe"):
            qametricpe = QAmetricPE(qareport)
            qametricpe.filename = filename
            qametricpe.datalabel = datalabel
            qametricpe.dra = get_value(pe.getElementsByTagName("dra"))
            qametricpe.draerr = get_value(pe.getElementsByTagName("draerr"))
            qametricpe.ddec = get_value(pe.getElementsByTagName("ddec"))
            qametricpe.ddecerr = get_value(pe.getElementsByTagName("ddecerr"))
            session.add(qametricpe)
            session.commit()

  except IOError:
    pass
  finally:
    session.close()

  return apache.OK

def get_value(element):
  try:
    return element[0].childNodes[0].data
  except:
    return None
   
