*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchHokupaaQUIRC
  [Documentation]  Search Hokupaa+QUIRC
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  Hokupaa+QUIRC
  input text  name=date  20020426
  click button  name=Search
  Page Should Contain  2002APR26_006.fits
