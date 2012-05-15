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

import json
import datetime
import dateutil.parser
import math

def qaforgui(req, things):
 
  """
  This function outputs a JSON dump, aimed at feeding the QA metric GUI display
  If you pass it a timestamp string in things, it will only list reports submitted after that timestamp
  """
  datestamp = None
  try:
    datestampstr = things[0]
    datestamp = dateutil.parser.parse(datestampstr)
  except (IndexError, ValueError):
    pass

  session = sessionfactory()
  try:
    req.content_type = "application/json"
    #req.content_type = "text/plain"

    # We only want the most recent of each value for each datalabel
    # Interested in IQ, ZP, BG

    # Get a list of datalabels
    iqquery = session.query(QAmetricIQ.datalabel)
    zpquery = session.query(QAmetricZP.datalabel)
    sbquery = session.query(QAmetricSB.datalabel)
    query = iqquery.union(zpquery).union(sbquery).distinct()
    datalabels = query.all()

    # Now loop through the datalabels. 
    # For each datalabel, get the most recent QA measurement of each type. Only ones reported after the datestamp
    for datalabel in datalabels:

      # Comes back as a 1 element list
      datalabel = datalabel[0]

      metadata = {}
      iq = {}
      cc = {}
      bg = {}
      metadata['datalabel']=datalabel

      # Fitst try and find the header entry for this datalabel, and populate what comes from that
      query = session.query(Header).select_from(Header, DiskFile).filter(Header.diskfile_id == DiskFile.id)
      query = query.filter(DiskFile.canonical == True).filter(Header.data_label == datalabel)
      header = query.first()
      # We can only populate the header info if it is in the header table
      if(header):
        metadata['filename']=header.diskfile.file.filename
        metadata['ut_time']=str(header.ut_datetime)
        metadata['local_time']=str(header.local_time)
        metadata['wavelength']=header.central_wavelength
        metadata['waveband']=header.wavelength_band
        metadata['airmass']=float(header.airmass)
        metadata['filter']=header.filter_name
        metadata['instrument']=header.instrument
        metadata['object']=header.object
        # Parse the types string back into a list using a locked-down eval
        metadata['types']=eval(header.types, {"__builtins__":None}, {})

      # Look for IQ metrics to report. Going to need to do the same merging trick here
      query = session.query(QAmetricIQ).select_from(QAmetricIQ, QAreport).filter(QAmetricIQ.qareport_id == QAreport.id)
      if(datestamp):
        query = query.filter_by(QAreport.submit_time > datestamp)
      query.filter(QAmetricIQ.datalabel == datalabel)
      query.order_by(desc(QAreport.submit_time))
      qaiq = query.first()

      # If we got anything, add it to the dict
      if(qaiq):
        iq['band']=qaiq.percentile_band
        iq['delivered']=float(qaiq.fwhm)
        iq['delivered_error']=float(qaiq.fwhm_std)
        iq['zenith']=float(qaiq.fwhm) * float(header.airmass)**(-0.6)
        iq['ellipticity']=float(qaiq.elip)
        iq['ellip_error']=float(qaiq.elip_std)
        iq['comment']=[qaiq.comment]
        if(header):
          iq['requested']=int(header.requested_iq)

      # Look for CC metrics to report. The DB has the different detectors in different entries, have to do some merging.
      # Find the qareport id of the most recent zp report for this datalabel
      query = session.query(QAreport).select_from(QAmetricZP, QAreport).filter(QAmetricZP.qareport_id == QAreport.id)
      if(datestamp):
        query = query.filter_by(QAreport.id.submit_time > datestamp)
      query.filter(QAmetricZP.datalabel == datalabel)
      query.order_by(desc(QAreport.submit_time))
      qarep = query.first()

      # Now find all the ZPmetrics for this qareport_id
      # By definition, it must be after the timestamp etc.
      zpmetrics=[]
      if(qarep):
        query = session.query(QAmetricZP).filter(QAmetricZP.qareport_id == qarep.id)
        zpmetrics = query.all()

      # Now go through those and merge them into the form required
      # This is a bit tediouos, given that we may have a result that is split by amp,
      # or we may have one from a mosaiced full frame image.
      cc_band=[]
      cc_zeropoint = {}
      cc_extinction = []
      cc_extinction_error = []
      cc_comment = []
      for z in zpmetrics:
        if z.percentile_band not in cc_band:
          cc_band.append(z.percentile_band)
        cc_extinction.append(float(z.cloud))
        cc_extinction_error.append(float(z.cloud_std))
        cc_zeropoint[z.detector]={'value':float(z.mag), 'error':float(z.mag_std)}
        if(z.comment not in cc_comment):
          cc_comment.append(z.comment)

      # Need to combine some of these to a single value
      cc['band'] = ', '.join(cc_band)
      cc['zeropoint']=cc_zeropoint
      if(len(cc_extinction)):
        cc['extinction'] = sum(cc_extinction) / len(cc_extinction)

        # Quick variance calculation, we could load numpy instead..
        s = 0
        for e in cc_extinction_error:
          s += e*e
        s /= len(cc_extinction_error)
        cc['extinction_error'] = math.sqrt(s)
      
      cc['comment'] = cc_comment
      if(header):
        cc['requested']=int(header.requested_cc)


      # Look for BG metrics to report. The DB has the different detectors in different entries, have to do some merging.
      # Find the qareport id of the most recent zp report for this datalabel
      query = session.query(QAreport).select_from(QAmetricSB, QAreport).filter(QAmetricSB.qareport_id == QAreport.id)
      if(datestamp):
        query = query.filter_by(QAreport.id.submit_time > datestamp)
      query.filter(QAmetricSB.datalabel == datalabel)
      query.order_by(desc(QAreport.submit_time))
      qarep = query.first()

      # Now find all the SBmetrics for this qareport_id
      # By definition, it must be after the timestamp etc.
      sbmetrics=[]
      if(qarep):
        query = session.query(QAmetricSB).filter(QAmetricSB.qareport_id == qarep.id)
        sbmetrics = query.all()

      # Now go through those and merge them into the form required
      # This is a bit tediouos, given that we may have a result that is split by amp,
      # or we may have one from a mosaiced full frame image.
      bg_band=[]
      bg_mag =[]
      bg_mag_std = []
      bg_comment = []
      for b in sbmetrics:
        if b.percentile_band not in bg_band:
          bg_band.append(b.percentile_band)
        bg_mag.append(float(b.mag))
        bg_mag_std.append(float(b.mag_std))
        if(b.comment not in bg_comment):
          bg_comment.append(b.comment)

      # Need to combine some of these to a single value
      bg['band'] = ', '.join(bg_band)
      if(len(bg_mag)):
        bg['brightness'] = sum(bg_mag) / len(bg_mag)

        # Quick variance calculation, we could load numpy instead..
        s = 0
        for e in bg_mag_std:
          s += e*e
        s /= len(bg_mag_std)
        bg['brightness_error'] = math.sqrt(s)
      
      bg['comment'] = bg_comment
      if(header):
        bg['requested']=int(header.requested_bg)

      # Now, put the stuff we built into a dict that we can push out to json
      dict={}
      if(len(metadata)):
        dict['metadata']=metadata
      if(len(iq)):
        dict['iq']=iq
      if(len(cc)):
        dict['cc']=cc
      if(len(bg)):
        dict['bg']=bg

      # Serialze it out via json to the request object 
      json.dump(dict, req, indent=4, )

  except IOError:
    pass
  finally:
    session.close()

  return apache.OK

