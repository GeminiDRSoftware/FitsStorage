*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
ViewCalibrations
  [Documentation]  View Calibrations
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=object  3C405
  input text  name=date  20130709-20130713
  click button  name=Search
  Page Should Contain  N20130711S0203.fits
  Click Link  id=caltab
  Sleep  30s  Waiting for tab to load
  Page Should Contain  GN-CAL20130710-22-033