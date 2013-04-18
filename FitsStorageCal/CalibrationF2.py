"""
This module holds the CalibrationF2 class
"""

import FitsStorageConfig
import GeminiMetadataUtils
from FitsStorage import DiskFile, Header, F2
from FitsStorageCal.Calibration import Calibration

from sqlalchemy.orm import join
from sqlalchemy import func, extract


class CalibrationF2(Calibration):
  """
  This class implements a calibration manager for F2.
  It is a subclass of Calibration
  """
  f2 = None

  def __init__(self, session, header, descriptors, types):
    # Init the superclass
    Calibration.__init__(self, session, header, descriptors, types)

    # Find the f2header
    query = session.query(F2).filter(F2.header_id==self.descriptors['header_id'])
    self.f2 = query.first()

    # Populate the descriptors dictionary for F2
    if(self.from_descriptors):
      self.descriptors['read_mode']=self.f2.read_mode
      self.descriptors['disperser']=self.f2.disperser
      self.descriptors['focal_plane_mask']=self.f2.focal_plane_mask
      self.descriptors['filter_name']=self.f2.filter_name
      self.descriptors['lyot_stop']=self.f2.lyot_stop

    # Set the list of required calibrations
    self.required = self.required()

  def required(self):
    # Return a list of the calibrations required for this dataset
    list=[]

    # Imaging OBJECTs require a DARK and a flat
    if((self.descriptors['observation_type']=='OBJECT') and (self.descriptors['spectroscopy']==False)):
      list.append('dark')
      list.append('flat')

    # Spectroscopy OBJECTs require a dark, flat and arc
    if((self.descriptors['observation_type']=='OBJECT') and (self.descriptors['spectroscopy']==True)):
      list.append('dark')
      list.append('flat')
      list.append('arc')

    # FLAT frames require DARKs
    if(self.descriptors['observation_type']=='FLAT'):
      list.append('dark')

    # ARCs require DARKs and FLATs
    if(self.descriptors['observation_type']=='ARC'):
      list.append('dark')
      list.append('flat')

    return list

  def dark(self, List=None):
    query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))
    query = query.filter(Header.observation_type=='DARK')

    # Search only canonical entries
    query = query.filter(DiskFile.canonical==True)

    # Knock out the FAILs
    query = query.filter(Header.qa_state!='Fail')

    # Must totally match: read_mode, exposure_time
    query = query.filter(F2.read_mode==self.descriptors['read_mode'])
    query = query.filter(Header.exposure_time==self.descriptors['exposure_time'])

    # Absolute time separation must be within 1 year (31557600 seconds)
    query = query.filter(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])) < 31557600)

    # Order by absolute time separation
    query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())
    
    # For now, we only want one result - the closest in time, unless otherwise indicated
    if(List):
      query = query.limit(List)
      return  query.all()
    else:
      query = query.limit(1)
      return query.first()

  def flat(self, List=None):
    query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))
    query = query.filter(Header.observation_type=='FLAT')

    # Search only canonical entries
    query = query.filter(DiskFile.canonical==True)

    # Knock out the FAILs
    query = query.filter(Header.qa_state!='Fail')

    # Must totally match: disperser, central_wavelength (spect only), focal_plane_mask, filter_name, lyot_stop, read_mode
    query = query.filter(F2.disperser==self.descriptors['disperser'])
    query = query.filter(F2.focal_plane_mask==self.descriptors['focal_plane_mask'])
    query = query.filter(F2.filter_name==self.descriptors['filter_name'])
    query = query.filter(F2.lyot_stop==self.descriptors['lyot_stop'])
    query = query.filter(F2.read_mode==self.descriptors['read_mode'])

    if(self.descriptors['spectroscopy']):
      query = query.filter(func.abs(Header.central_wavelength - self.descriptors['central_wavelength']) < 0.001)

    # Absolute time separation must be within 1 year (31557600 seconds)
    query = query.filter(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])) < 31557600)

    # Order by absolute time separation
    query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())

    # For now, we only want one result - the closest in time, unless otherwise indicated
    if(List):
      query = query.limit(List)
      return  query.all()
    else:
      query = query.limit(1)
      return query.first()

  def arc(self, sameprog=False, List=None):
    query = self.session.query(Header).select_from(join(join(F2, Header), DiskFile))
    query = query.filter(Header.observation_type=='ARC')

    # Search only the canonical (latest) entries
    query = query.filter(DiskFile.canonical==True)

    # Knock out the FAILs
    query = query.filter(Header.qa_state!='Fail')

    # Must Totally Match: disperser, central_wavelength, focal_plane_mask, filter_name, lyot_stop
    query = query.filter(F2.disperser==self.descriptors['disperser'])
    query = query.filter(func.abs(Header.central_wavelength - self.descriptors['central_wavelength']) < 0.001)
    query = query.filter(F2.focal_plane_mask==self.descriptors['focal_plane_mask'])
    query = query.filter(F2.filter_name==self.descriptors['filter_name'])
    query = query.filter(F2.lyot_stop==self.descriptors['lyot_stop'])

    # Absolute time separation must be within 1 year (31557600 seconds)
    query = query.filter(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])) < 31557600)

    # Order by absolute time separation
    query = query.order_by(func.abs(extract('epoch', Header.ut_datetime - self.descriptors['ut_datetime'])).asc())

    # For now, we only want one result - the closest in time, unless otherwise indicated
    if(List):
      query = query.limit(List)
      return  query.all()
    else:
      query = query.limit(1)
      return query.first()
