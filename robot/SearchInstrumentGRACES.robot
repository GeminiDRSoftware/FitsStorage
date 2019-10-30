*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchGRACES
  [Documentation]  Search GRACES
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GRACES
  input text  name=date  20190628
  click button  name=Search
  Page Should Contain  N20190628G0024.fits

