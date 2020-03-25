*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       http://fits.hi.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
QAForGUI
  [Documentation]  Web API link for QA GUI
  Open Browser  ${URL}/calibrations/GMOS/NotFail/20191216/arc/warnings  ${BROWSER}
  Page Should Contain  Calibrations
