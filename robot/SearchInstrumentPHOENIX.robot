*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchPHOENIX
  [Documentation]  Search PHOENIX
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  PHOENIX
  input text  name=date  20190322
  click button  name=Search
  Page Should Contain  2019mar22_science018.fits
