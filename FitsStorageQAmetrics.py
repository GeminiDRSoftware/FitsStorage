"""
This module contains the QA metric database interface
"""

from FitsStorage import *
from FitsStorageConfig import fsc_localmode
import urllib
from xml.dom.minidom import parseString
import ApacheReturnCodes as apache

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
          detector = get_value(qam.getElementsByTagName("detector"))
          for iq in qam.getElementsByTagName("iq"):
            qametriciq = QAmetricIQ(qareport)
            qametriciq.datalabel = datalabel
            qametriciq.filename = filename
            qametriciq.detector = detector
            qametriciq.fwhm = get_value(iq.getElementsByTagName("fwhm"))
            qametriciq.fwhm_std = get_value(iq.getElementsByTagName("fwhm_std"))
            qametriciq.isofwhm = get_value(iq.getElementsByTagName("isofwhm"))
            qametriciq.isofwhm_std = get_value(iq.getElementsByTagName("isofwhm_std"))
            qametriciq.ee50d = get_value(iq.getElementsByTagName("ee50d"))
            qametriciq.ee50d_std = get_value(iq.getElementsByTagName("ee50d_std"))
            qametriciq.elip = get_value(iq.getElementsByTagName("elip"))
            qametriciq.elip_std = get_value(iq.getElementsByTagName("elip_std"))
            qametriciq.pa = get_value(iq.getElementsByTagName("pa"))
            qametriciq.pa_std = get_value(iq.getElementsByTagName("pa_std"))
            qametriciq.strehl = get_value(iq.getElementsByTagName("strehl"))
            qametriciq.strehl_std = get_value(iq.getElementsByTagName("strehl_std"))
            qametriciq.nsamples = get_value(iq.getElementsByTagName("nsamples"))
            qametriciq.percentile_band = get_value(iq.getElementsByTagName("percentile_band"))
            qametriciq.comment = get_value(iq.getElementsByTagName("comment"))
            session.add(qametriciq)
            session.commit()
          for zp in qam.getElementsByTagName("zp"):
            qametriczp = QAmetricZP(qareport)
            qametriczp.datalabel = datalabel
            qametriczp.filename = filename
            qametriczp.detector = detector
            qametriczp.mag = get_value(zp.getElementsByTagName("mag"))
            qametriczp.mag_std = get_value(zp.getElementsByTagName("mag_std"))
            qametriczp.cloud = get_value(zp.getElementsByTagName("cloud"))
            qametriczp.cloud_std = get_value(zp.getElementsByTagName("cloud_std"))
            qametriczp.photref = get_value(zp.getElementsByTagName("photref"))
            qametriczp.nsamples = get_value(zp.getElementsByTagName("nsamples"))
            qametriczp.percentile_band = get_value(zp.getElementsByTagName("percentile_band"))
            qametriczp.comment = get_value(zp.getElementsByTagName("comment"))
            session.add(qametriczp)
            session.commit()
          for sb in qam.getElementsByTagName("sb"):
            qametricsb = QAmetricSB(qareport)
            qametricsb.filename = filename
            qametricsb.datalabel = datalabel
            qametricsb.detector = detector
            qametricsb.mag = get_value(sb.getElementsByTagName("mag"))
            qametricsb.mag_std = get_value(sb.getElementsByTagName("mag_std"))
            qametricsb.electrons = get_value(sb.getElementsByTagName("elecrons"))
            qametricsb.electrons_std = get_value(sb.getElementsByTagName("elecrons_std"))
            qametricsb.nsamples = get_value(sb.getElementsByTagName("nsamples"))
            qametricsb.comment = get_value(sb.getElementsByTagName("comment"))
            qametricsb.percentile_band = get_value(sb.getElementsByTagName("percentile_band"))
            session.add(qametricsb)
            session.commit()
          for pe in qam.getElementsByTagName("pe"):
            qametricpe = QAmetricPE(qareport)
            qametricpe.filename = filename
            qametricpe.datalabel = datalabel
            qametricpe.detector = detector
            qametricpe.dra = get_value(pe.getElementsByTagName("dra"))
            qametricpe.dra_std = get_value(pe.getElementsByTagName("dra_std"))
            qametricpe.ddec = get_value(pe.getElementsByTagName("ddec"))
            qametricpe.ddec_std = get_value(pe.getElementsByTagName("ddec_std"))
            qametricpe.astref = get_value(pe.getElementsByTagName("astref"))
            qametricpe.nsamples = get_value(pe.getElementsByTagName("nsamples"))
            qametricpe.comment = get_value(pe.getElementsByTagName("comment"))
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
   

def qametrics(req, things):
  """
  This function is the initial, simple display of QA metric data
  """
  session = sessionfactory()
  try:

    req.content_type = "text/plain"
    if ('iq' in things):
      query = session.query(QAmetricIQ).select_from(QAmetricIQ, QAreport).filter(QAmetricIQ.qareport_id == QAreport.id)
      qalist = query.all()

      req.write("#Datalabel, filename, detector, filter, utdatetime, Nsamples, FWHM, FWHM_std, isoFWHM, isoFWHM_std, EE50d, EE50d_std, elip, elip_std, pa, pa_std, strehl, strehl_std, percentile_band, comments\n")

      for qa in qalist:
        hquery = session.query(Header).select_from(Header, DiskFile).filter(Header.diskfile_id == DiskFile.id).filter(DiskFile.canonical == True)
        hquery = hquery.filter(Header.data_label == qa.datalabel)
        header = hquery.first()
        if(header):
          filter = header.filter_name
          utdt = header.ut_datetime
        else:
          filter = None
          utdt = None

        req.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % (qa.datalabel, qa.filename, qa.detector, filter, utdt, qa.nsamples, qa.fwhm, qa.fwhm_std, qa.isofwhm, qa.isofwhm_std, qa.ee50d, qa.ee50d_std, qa.elip, qa.elip_std, qa.pa, qa.pa_std, qa.strehl, qa.strehl_std, qa.percentile_band, qa.comment))
     
      req.write("#---------") 

    if ('zp' in things):
      query = session.query(QAmetricZP).select_from(QAmetricZP, QAreport).filter(QAmetricZP.qareport_id == QAreport.id)
      qalist = query.all()

      req.write("#Datalabel, filename, detector, filter, utdatetime, Nsamples, zp_mag, zp_mag_std, cloud, cloud_std, photref, percentile_band, comment\n")

      for qa in qalist:
        hquery = session.query(Header).select_from(Header, DiskFile).filter(Header.diskfile_id == DiskFile.id).filter(DiskFile.canonical == True)
        hquery = hquery.filter(Header.data_label == qa.datalabel)
        header = hquery.first()
        if(header):
          filter = header.filter_name
          utdt = header.ut_datetime
        else:
          filter = None
          utdt = None

        req.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % (qa.datalabel, qa.filename, qa.detector, filter, utdt, qa.nsamples, qa.mag, qa.mag_std, qa.cloud, qa.cloud_std, qa.photref, qa.percentile_band, qa.comment))
     
      req.write("#---------") 


    if ('sb' in things):
      query = session.query(QAmetricSB).select_from(QAmetricSB, QAreport).filter(QAmetricSB.qareport_id == QAreport.id)
      qalist = query.all()

      req.write("#Datalabel, filename, detector, filter, utdatetime, Nsamples, sb_mag, sb_mag_std, sb_electrons, sb_electrons_std, percentile_band, comment\n")

      for qa in qalist:
        hquery = session.query(Header).select_from(Header, DiskFile).filter(Header.diskfile_id == DiskFile.id).filter(DiskFile.canonical == True)
        hquery = hquery.filter(Header.data_label == qa.datalabel)
        header = hquery.first()
        if(header):
          filter = header.filter_name
          utdt = header.ut_datetime
        else:
          filter = None
          utdt = None

        req.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % (qa.datalabel, qa.filename, qa.detector, filter, utdt, qa.nsamples, qa.mag, qa.mag_std, qa.electrons, qa.electrons_std, qa.percentile_band, qa.comment))
     
      req.write("#---------") 

    if ('pe' in things):
      query = session.query(QAmetricPE).select_from(QAmetricPE, QAreport).filter(QAmetricPE.qareport_id == QAreport.id)
      qalist = query.all()

      req.write("#Datalabel, filename, detector, filter, utdatetime, Nsamples, dra, dra_std, ddec, ddec_std, astref, comment\n")

      for qa in qalist:
        hquery = session.query(Header).select_from(Header, DiskFile).filter(Header.diskfile_id == DiskFile.id).filter(DiskFile.canonical == True)
        hquery = hquery.filter(Header.data_label == qa.datalabel)
        header = hquery.first()
        if(header):
          filter = header.filter_name
          utdt = header.ut_datetime
        else:
          filter = None
          utdt = None

        req.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % (qa.datalabel, qa.filename, qa.detector, filter, utdt, qa.nsamples, qa.dra, qa.dra_std, qa.ddec, qa.ddec_std, qa.astref, qa.comment))

      req.write("#---------")


  except IOError:
    pass
  finally:
    session.close()

  return apache.OK
