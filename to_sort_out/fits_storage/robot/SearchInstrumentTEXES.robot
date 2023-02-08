*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchTEXES
  [Documentation]  Search TEXES
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  TEXES
  input text  name=date  20170321
  click button  name=Search
  Page Should Contain  TX20170321_raw.2504.fits
