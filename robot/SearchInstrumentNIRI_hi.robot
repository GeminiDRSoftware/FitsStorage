*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchNIRI
  [Documentation]  Search NIRI
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20190701
  click button  name=Search
  Page Should Contain  N20190701S0019.fits

SearchNIRIFilter
  [Documentation]  Search NIRI with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20190701
  Select From List By Value  id:niri_filter  J
  click button  name=Search
  Page Should Contain  N20190701S0724.fits
