*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
FullHeader
  [Documentation]  Get Full Header
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=date  20120629
  input text  name=object  m101
  click button  name=Search
  click link  N20120629S0171.fits
  Page Should Contain  GN-2012A-Q-49-67-001

