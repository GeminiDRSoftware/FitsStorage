import re

# This first block of regexps are compiled here but used elsewhere
percentilecre=re.compile('^\d\d-percentile$')

# Compile some regular expressions here. This is fairly complex, so I've
# split it up in substrings to make it easier to follow.
# Also these substrings are used directly by the classes

# This re matches a program id like GN-CAL20091020 with no groups
calengre='G[NS]-(?:(?:CAL)|(?:ENG))20\d\d[01]\d[0123]\d'
# This re matches a program id like GN-2009A-Q-23 with no groups
scire='G[NS]-20\d\d[AB]-[A-Z]*-\d*'

# This matches a program id
progre='(?:^%s$)|(?:^%s$)' % (calengre, scire)

# This matches an observation id with the project id and obsnum as groups
obsre='((?:^%s)|(?:^%s))-(\d*)$' % (calengre, scire)

# The Gemini Data Label Class

# This re matches progid-obsum-dlnum - ie a datalabel,
# With 3 groups - progid, obsnum, dlnum
dlcre=re.compile('^((?:%s)|(?:%s))-(\d*)-(\d*)$' % (calengre, scire))

class GeminiDataLabel:
  datalabel = ''
  projectid = ''
  obsid = ''
  obsnum = ''
  dlnum = ''
  project = ''

  def __init__(self, dl):
    self.datalabel = dl
    self.projectid = ''
    self.obsid = ''
    self.obsnum = ''
    self.dlnum = ''
    if(self.datalabel):
      self.parse()

  def parse(self):
    dlm=dlcre.match(self.datalabel)
    if(dlm):
      self.projectid = dlm.group(1)
      self.obsnum = dlm.group(2)
      self.dlnum = dlm.group(3)
      self.project = GeminiProject(self.projectid)
      self.obsid='%s-%s' % (self.projectid, self.obsnum)
    else:
      # Match failed - Null the datalabel field
      self.datalabel=''

# This matches an observation id
obscre = re.compile(obsre)

class GeminiObservation:
  obsid = ''
  project = ''
  obsnum =''

  def __init__(self, obsid):
    if(obsid):
      match = obscre.match(obsid)
      if(match):
        self.obsid = obsid
        self.project = GeminiProject(match.group(1))
        self.obsnum = match.group(2)
      else:
        self.obsid = ''
        self.project=''
        self.obsnum=''
    else:
      self.obsid = ''

# This matches a program id
progcre=re.compile(progre)

# this matches a cal or eng projectid with CAL or ENG and the date as matched groups
cecre=re.compile('G[NS]-((?:CAL)|(?:ENG))(20\d\d[01]\d[0123]\d)')

class GeminiProject:
  progid = ''
  iscal = ''
  iseng = ''

  def __init__(self, progid):
    if(progcre.match(progid)):
      self.progid = progid
      self.parse()
    else:
      self.progid=''
      iscal = False
      iseng = False

  def parse(self):
    cem=cecre.match(self.progid)
    if(cem):
      caleng = cem.group(1)
      self.iseng = (caleng == 'ENG')
      self.iscal = (caleng == 'CAL')

