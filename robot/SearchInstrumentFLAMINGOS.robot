*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchFLAMINGS
  [Documentation]  Search FLAMINGOS
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  FLAMINGOS
  input text  name=date  20021112
  click button  name=Search
  Page Should Contain  02nov12.0440.fits
