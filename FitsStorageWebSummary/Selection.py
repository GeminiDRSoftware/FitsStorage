"""
This module deals with the 'selection' concept.
Functions in this module are only used within FitsStorageWebSummary.
"""
from sqlalchemy import or_
from FitsStorage import *
from GeminiMetadataUtils import *
from FitsStorageConfig import *

def getselection(things):
  # this takes a list of things from the URL, and returns a
  # selection hash that is used by the html generators
  selection = {}
  while(len(things)):
    thing = things.pop(0)
    recognised=False
    if(gemini_date(thing)):
      selection['date']=gemini_date(thing)
      recognised=True
    if(gemini_daterange(thing)):
      selection['daterange']=gemini_daterange(thing)
      recognised=True
    gp=GeminiProject(thing)
    if(gp.program_id):
      selection['program_id']=thing
      recognised=True
    go=GeminiObservation(thing)
    if(go.observation_id):
      selection['observation_id']=thing
      recognised=True
    gdl=GeminiDataLabel(thing)
    if(gdl.datalabel):
      selection['data_label']=thing
      recognised=True
    if(gemini_instrument(thing, gmos=True)):
      selection['inst']=gemini_instrument(thing, gmos=True)
      recognised=True
    if(gemini_fitsfilename(thing)):
      selection['filename'] = gemini_fitsfilename(thing)
      recognised=True
    if(gemini_observation_type(thing)):
      selection['observation_type']=gemini_observation_type(thing)
      recognised=True
    if(gemini_observation_class(thing)):
      selection['observation_class']=gemini_observation_class(thing)
      recognised=True
    if(gemini_caltype(thing)):
      selection['caltype']=gemini_caltype(thing)
      recognised=True
    if(gemini_reduction_state(thing)):
      selection['reduction']=gemini_reduction_state(thing)
      recognised=True
    if(gmos_gratingname(thing)):
      selection['gmos_grating']=gmos_gratingname(thing)
      recognised=True
    if(gmos_focal_plane_mask(thing)):
      selection['gmos_focal_plane_mask']=gmos_focal_plane_mask(thing)
      recognised=True
    if(thing=='warnings' or thing=='missing' or thing=='requires' or thing=='takenow'):
      selection['caloption']=thing
      recognised=True
    if(thing=='imaging' or thing=='Imaging'):
      selection['spectroscopy']=False
      recognised=True
    if(thing=='spectroscopy' or thing=='Spectroscopy'):
      selection['spectroscopy']=True
      recognised=True
    if(thing=='Pass' or thing=='Usable' or thing=='Fail' or thing=='Win' or thing=='NotFail'):
      selection['qa_state']=thing
      recognised=True
    if(thing=='AO' or thing=='NOTAO'):
      selection['ao']=thing
      recognised=True
    if(thing=='present' or thing=='Present'):
      selection['present']=True
      recognised=True
    if(thing=='notpresent' or thing=='NotPresent'):
      selection['present']=False
      recognised=True
    if(thing=='canonical' or thing=='Canonical'):
      selection['canonical']=True
      recognised=True
    if(thing=='notcanonical' or thing=='NotCanonical'):
      selection['canonical']=False
      recognised=True
    if(thing[:7]=='filter=' or thing[:7]=='Filter='):
      selection['filter']=thing[7:]
      recognised=True
    if(gemini_binning(thing)):
      selection['binning']=gemini_binning(thing)
      recognised=True
    if(thing=='photstandard'):
      selection['photstandard']=True
      recognised=True
    if(thing in ['low', 'high', 'slow', 'fast']):
      if(not selection.has_key('detector_config')):
        selection['detector_config']=[]
      selection['detector_config'].append(thing)
      recognised=True
    if(thing=='Twilight'):
      selection['twilight']=True
      recognised=True
    if(thing=='NotTwilight'):
      selection['twilight']=False
      recognised=True

    if(not recognised):
      if('notrecognised' in selection):
        selection['notrecognised'] += " "+thing
      else:
        selection['notrecognised'] = thing
  return selection

def sayselection(selection):
  """
  returns a string that describes the selection dictionary passed in
  suitable for pasting into html
  """
  string = ""

  defs = {'program_id': 'Program ID', 'observation_id': 'Observation ID', 'data_label': 'Data Label', 'date': 'Date', 'daterange': 'Daterange', 'inst':'Instrument', 'observation_type':'ObsType', 'observation_class': 'ObsClass', 'filename': 'Filename', 'gmos_grating': 'GMOS Grating', 'gmos_focal_plane_mask': 'GMOS FP Mask', 'binning': 'Binning', 'caltype': 'Calibration Type', 'caloption': 'Calibration Option', 'photstandard': 'Photometric Standard', 'reduction': 'Reduction State', 'twilight': 'Twilight'}
  for key in defs:
    if key in selection:
      string += "; %s: %s" % (defs[key], selection[key])

  if('spectroscopy' in selection):
    if(selection['spectroscopy']):
      string += "; Spectroscopy"
    else:
      string += "; Imaging"
  if('qa_state' in selection):
    if(selection['qa_state']=='Win'):
      string += "; QA State: Win (Pass or Usable)"
    elif(selection['qa_state']=='NotFail'):
      string += "; QA State: Not Fail"
    else:
      string += "; QA State: %s" % selection['qa_state']
  if('ao' in selection):
    if(selection['ao']=='AO'):
      string += "; Adaptive Optics in beam"
    else:
      string += "; No Adaptive Optics in beam"
  if('detector_config' in selection):
    string += "; Detector Config: " + '+'.join(selection['detector_config'])

  if('notrecognised' in selection):
    string += ". WARNING: I didn't understand these (case-sensitive) words: %s" % selection['notrecognised']

  return string

# import time module to get local timezone
import time
def queryselection(query, selection):
  """
  Given an sqlalchemy query object and a selection dictionary,
  add filters to the query for the items in the selection
  and return the query object
  """

  # Do want to select Header object for which diskfile.present is true?
  if('present' in selection):
    query = query.filter(DiskFile.present==selection['present'])

  if('canonical' in selection):
    query = query.filter(DiskFile.canonical==selection['canonical'])

  if('program_id' in selection):
    query = query.filter(Header.program_id==selection['program_id'])

  if('observation_id' in selection):
    query = query.filter(Header.observation_id==selection['observation_id'])

  if('data_label' in selection):
    query = query.filter(Header.data_label==selection['data_label'])

  # Should we query by date?
  if('date' in selection):
    # Parse the date to start and end datetime objects
    # We consider the night boundary to be 14:00 local time
    # This is midnight UTC in Hawaii, completely arbitrary in Chile
    startdt = dateutil.parser.parse("%s 14:00:00" % (selection['date']))
    tzoffset = datetime.timedelta(seconds=time.timezone)
    oneday = datetime.timedelta(days=1)
    startdt = startdt + tzoffset - oneday
    enddt = startdt + oneday
    # check it's between these two
    query = query.filter(Header.ut_datetime >= startdt).filter(Header.ut_datetime < enddt)

  # Should we query by daterange?
  if('daterange' in selection):
    # Parse the date to start and end datetime objects
    daterangecre=re.compile('(20\d\d[01]\d[0123]\d)-(20\d\d[01]\d[0123]\d)')
    m = daterangecre.match(selection['daterange'])
    startdate = m.group(1)
    enddate = m.group(2)
    tzoffset = datetime.timedelta(seconds=time.timezone)
    oneday = datetime.timedelta(days=1)
    startdt = dateutil.parser.parse("%s 14:00:00" % startdate)
    startdt = startdt + tzoffset - oneday
    enddt = dateutil.parser.parse("%s 14:00:00" % enddate)
    enddt = enddt + tzoffset - oneday
    enddt = enddt + oneday
    # Flip them round if reversed
    if(startdt > enddt):
      tmp = enddt
      enddt = startdt
      started = tmp
    # check it's between these two
    query = query.filter(Header.ut_datetime >= startdt).filter(Header.ut_datetime <= enddt)

  if('observation_type' in selection):
    query = query.filter(Header.observation_type==selection['observation_type'])

  if('observation_class' in selection):
    query = query.filter(Header.observation_class==selection['observation_class'])

  if('reduction' in selection):
    query = query.filter(Header.reduction==selection['reduction'])

  if('inst' in selection):
    if(selection['inst']=='GMOS'):
      query = query.filter(or_(Header.instrument=='GMOS-N', Header.instrument=='GMOS-S'))
    else:
      query = query.filter(Header.instrument==selection['inst'])

  if('filename' in selection):
    query = query.filter(File.filename==selection['filename'])

  if('gmos_grating' in selection):
    query = query.filter(Header.disperser==selection['gmos_grating'])

  if('gmos_focal_plane_mask' in selection):
    query = query.filter(Header.focal_plane_mask==selection['gmos_focal_plane_mask'])

  if('spectroscopy' in selection):
    query = query.filter(Header.spectroscopy==selection['spectroscopy'])

  if('qa_state' in selection):
    if(selection['qa_state']=='Win'):
      query = query.filter(or_(Header.qa_state=='Pass', Header.qa_state=='Usable'))
    elif(selection['qa_state']=='NotFail'):
      query = query.filter(Header.qa_state!='Fail')
    else:
      query = query.filter(Header.qa_state==selection['qa_state'])

  if('ao' in selection):
    if(selection['ao']=='AO'):
      query = query.filter(Header.adaptive_optics==True)
    else:
      query = query.filter(Header.adaptive_optics==False)

  if('binning' in selection):
    query = query.filter(Header.detector_binning==selection['binning'])

  if('filter' in selection):
    query = query.filter(Header.filter_name==selection['filter'])

  if('photstandard' in selection):
    query = query.filter(Footprint.header_id == Header.id)
    query = query.filter(PhotStandardObs.footprint_id == Footprint.id)

  if('detector_config' in selection):
    for thing in selection['detector_config']:
      query = query.filter(Header.detector_config.like('%'+thing+'%'))

  if('twilight' in selection):
    if(selection['twilight'] == True):
      query = query.filter(Header.object == 'Twilight')
    if(selection['twilight'] == False):
      query = query.filter(Header.object != 'Twilight')

  return query

def openquery(selection):
  """
  Returns a boolean to say if the selection is limited to a reasonable number of
  results - ie does it contain a date, daterange, prog_id, obs_id etc
  returns True if this selection will likely return a large number of results
  """
  openquery = True

  things = ['date', 'daterange', 'program_id', 'observation_id', 'data_label', 'filename']

  for thing in things:
    if(thing in selection):
      openquery = False

  return openquery
