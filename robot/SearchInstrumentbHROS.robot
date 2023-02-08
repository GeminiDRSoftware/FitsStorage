*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchbHROS
  [Documentation]  Search bHROS
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  bHROS
  input text  name=date  20070131
  click button  name=Search
  Page Should Contain  S20070131S0145.fits
