*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchGSAOI
  [Documentation]  Search GSAOI
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GSAOI
  input text  name=date  20130201
  click button  name=Search
  Page Should Contain  S20130201S0158.fits

SearchGSAOIFilter
  [Documentation]  Search GSAOI with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GSAOI
  input text  name=date  20130201
  Select From List By Value  id:gsaoi_filter  Kshort
  click button  name=Search
  Page Should Contain  S20130201S0173.fits

SearchGSAOIExposureTime
  [Documentation]  Search GSAOI with Exposure Time
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GSAOI
  input text  name=date  20130201
  input text  id=GSAOIexpT  12
  click button  name=Search
  Page Should Contain  S20130202S0349.fits

