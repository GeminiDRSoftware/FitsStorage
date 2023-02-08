*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchCIRPASS
  [Documentation]  Search CIRPASS
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  CIRPASS
  input text  name=date  20030724
  click button  name=Search
  Page Should Contain  2003jun29_3341.fits
