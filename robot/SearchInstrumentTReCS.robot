*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchTReCS
  [Documentation]  Search TReCS
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  TReCS
  input text  name=date  20120605
  click button  name=Search
  Page Should Contain  rS20120605S0120.fits

SearchTReCSFilter
  [Documentation]  Search TReCS with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  TReCS
  input text  name=date  20120605
  Select From List By Value  id:trecs_filter  Si3-9.7um
  click button  name=Search
  Page Should Contain  S20120605S0127.fits

SearchTReCSDisperser
  [Documentation]  Search TReCS with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  TReCS
  input text  name=date  20111124
  Select From List By Value  id:trecs_disperser  LowRes-20
  click button  name=Search
  Page Should Contain  S20111124S0069.fits

#SearchTReCSMask
#  [Documentation]  Search TReCS with Mask
#  Open Browser  ${URL}/searchform  ${BROWSER}
#  Page Should Contain  PI/CoI Name
#  Select From List By Value  id:instselect  TReCS
#  input text  name=date  20111124
#  Select From List By Value  id:trecs_mask  ???
#  click button  name=Search
#  Page Should Contain  N20111109S0176.fits
