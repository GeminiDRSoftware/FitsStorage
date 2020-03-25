*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       http://fits.hi.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
QAForGUI
  [Documentation]  Web API link for QA GUI
  Open Browser  ${URL}/qaforgui/20191216  ${BROWSER}
  Page Should Contain  metadata
