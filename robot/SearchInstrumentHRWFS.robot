*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchHRWFS
  [Documentation]  Search HRWFS/AcqCam
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  hrwfs
  input text  name=date  20060804
  click button  name=Search
  Page Should Contain  2006aug04_1.3042.fits
