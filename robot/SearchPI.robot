*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchPICoIName
  [Documentation]  Search PI/CoI Name
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=PIname  Hirst
  input text  name=date  20130711
  click button  name=Search
  Page Should Contain  N20130711S0203.fits

