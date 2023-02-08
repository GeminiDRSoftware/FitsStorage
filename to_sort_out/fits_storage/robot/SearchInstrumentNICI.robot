*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchNICI
  [Documentation]  Search NICI
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NICI
  input text  name=date  20130805-20130806
  click button  name=Search
  Page Should Contain  S20130806S0071.fits

SearchNICIFilter
  [Documentation]  Search NICI with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NICI
  input text  name=date  20120401
  Select From List By Value  id:nici_filter  Lprime+CH4-H1S
  click button  name=Search
  Page Should Contain  S20120401S0218.fits

