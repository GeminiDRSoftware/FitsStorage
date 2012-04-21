"""
This is the Fits Storage Web Summary module. It provides the functions
which query the database and generate html for the web header
summaries.
"""
from FitsStorage import *
from mod_python import apache

def list_phot_std_obs(session, header_id):
  """
  Returns a list of the photometric standards that should be in this header id
  """

  query = session.query(PhotStandardObs.photstandard_id).select_from(PhotStandardObs, Footprint)
  query = query.filter(PhotStandardObs.footprint_id == Footprint.id)
  query = query.filter(Footprint.header_id == header_id)

  list = query.all()

  return list

def standardobs(req, header_id):
  """
  sends and html table detailing the standard stars visisble in this header_id
  """

  session = sessionfactory()
  try:
    req.content_type = "text/html"
    req.write("<html>")
    title = "Photometric standards in frame"
    req.write("<head>")
    req.write("<title>%s</title>" % (title))
    req.write('<link rel="stylesheet" href="/htmldocs/table.css">')
    req.write("</head>\n")
    req.write("<body>")
    req.write("<H1>%s</H1>" % (title))
  
    req.write('<TABLE border=0>')
  
    req.write('<TR class=tr_head>')
    req.write('<TH>Name</TH>')
    req.write('<TH>Field</TH>')
    req.write('<TH>RA</TH>')
    req.write('<TH>Dec</TH>')
    req.write('<TH>u_mag</TH>')
    req.write('<TH>v_mag</TH>')
    req.write('<TH>g_mag</TH>')
    req.write('<TH>r_mag</TH>')
    req.write('<TH>i_mag</TH>')
    req.write('<TH>z_mag</TH>')
    req.write('<TH>y_mag</TH>')
    req.write('<TH>j_mag</TH>')
    req.write('<TH>h_mag</TH>')
    req.write('<TH>k_mag</TH>')
    req.write('<TH>l_prime_mag</TH>')
    req.write('<TH>m_mag</TH>')
  
    list = list_phot_std_obs(session, header_id)
  
    even=0
    for std_id in list:
      std = session.query(PhotStandard).filter(PhotStandard.id == std_id).one()
      even = not even
      if(even):
        cs = "tr_even"
      else:
        cs = "tr_odd"
      req.write("<TR class=%s>" % (cs))
      req.write("<TD>%s</TD>" % std.name)
      req.write("<TD>%s</TD>" % std.field)
      req.write("<TD>%f</TD>" % std.ra)
      req.write("<TD>%f</TD>" % std.dec)
      try:
        req.write("<TD>%f</TD>" % std.u_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.v_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.g_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.r_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.i_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.z_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.y_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.j_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.h_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.k_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.lprime_mag)
      except TypeError:
        req.write("<TD></TD>")
      try:
        req.write("<TD>%f</TD>" % std.m_mag)
      except TypeError:
        req.write("<TD></TD>")
      req.write("</TR>")
  
    req.write("</TABLE>")
    req.write("</body></html>")
  
    return apache.OK

  finally:
    session.close()

def xmlstandardobs(req, header_id):
  """
  Writes xml fragment defining the standards visible in this header_id
  """

  session = sessionfactory()
  try:
    list = list_phot_std_obs(session, header_id)
    for std_id in list:
      std = session.query(PhotStandard).filter(PhotStandard.id == std_id).one()
      req.write("<photstandard>")
      req.write("<name>%s</name>" % std.name)
      req.write("<field>%s</field>" % std.field)
      req.write("<ra>%f</ra>" % std.ra)
      req.write("<dec>%f</dec>" % std.dec)
      if(std.u_mag):
        req.write("<u_mag>%f</u_mag>" % std.u_mag)
      if(std.v_mag):
        req.write("<v_mag>%f</v_mag>" % std.v_mag)
      if(std.g_mag):
        req.write("<g_mag>%f</g_mag>" % std.g_mag)
      if(std.r_mag):
        req.write("<r_mag>%f</r_mag>" % std.r_mag)
      if(std.i_mag):
        req.write("<i_mag>%f</i_mag>" % std.i_mag)
      if(std.z_mag):
        req.write("<z_mag>%f</z_mag>" % std.z_mag)
      if(std.y_mag):
        req.write("<y_mag>%f</y_mag>" % std.y_mag)
      if(std.j_mag):
        req.write("<j_mag>%f</j_mag>" % std.j_mag)
      if(std.h_mag):
        req.write("<h_mag>%f</h_mag>" % std.h_mag)
      if(std.k_mag):
        req.write("<k_mag>%f</k_mag>" % std.k_mag)
      if(std.lprime_mag):
        req.write("<lprime_mag>%f</lprime_mag>" % std.lprime_mag)
      if(std.m_mag):
        req.write("<m_mag>%f</m_mag>" % std.m_mag)
      req.write("</photstandard>")


  finally:
    session.close()

