*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchOSCIR
  [Documentation]  Search OSCIR
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  OSCIR
  input text  name=date  20011210
  click button  name=Search
  Page Should Contain  r01dec10_008.fits
