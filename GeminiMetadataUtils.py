# The Gemini Data Label Class

# Compile the regular expression here. This is fairly complex, so I've
# split it up in substrings to make it easier to follow
# This re matches a program id like GN-CAL20091020 with no groups
calengre='G[NS]-(?:(?:CAL)|(?:ENG))20\d\d[01]\d[0123]\d'
# This re matches a program id like GN-2009A-Q-23 with no groups
scire='G[NS]-20\d\d[AB]-\S*-\d*'
# This re matches progid-obsum-dlnum - ie a datalabel,
# With 3 groups - progid, obsnum, dlnum
dlcre=re.compile('((?:%s)|(?:%s))-(\d*)-(\d*)' % (calengre, scire))

# this matches a cal or eng projectid with CAL or ENG and the date as matched groups
cecre=re.compile('G[NS]-((?:CAL)|(?:ENG))(20\d\d[01]\d[0123]\d)')

class GeminiDataLabel:
  datalabel = ''
  projectid = ''
  obsid = ''
  obsnum = ''
  dlnum = ''
  iseng = False
  iscal = False

  def __init__(self, dl):
    self.datalabel = dl
    self.projectid = ''
    self.obsid = ''
    self.obsnum = ''
    self.dlnum = ''
    self.iseng = False
    self.iscal = False
    if(self.datalabel):
      self.parse()

  def parse(self):
    dlm=dlcre.match(self.datalabel)
    if(dlm):
      self.projectid = dlm.group(1)
      self.obsnum = dlm.group(2)
      self.dlnum = dlm.group(3)
      cem=cecre.match(self.projectid)
      if(cem):
        caleng = cem.group(1)
        iseng = (caleng == 'ENG')
        iscal = (caleng == 'CAL')
      self.obsid='%s-%s' % (self.projectid, self.obsnum)
    else:
      # Match failed - Null the datalabel field
      self.datalabel=''

