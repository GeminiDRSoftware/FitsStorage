*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
LoadObsLogs
  [Documentation]  Load Associated Observation Logs
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=PIname  Hirst
  input text  name=date  20130710-20130712
  click button  name=Search
  Sleep  10s  Waiting for tab to load
  Page Should Contain  N20130711S0203.fits
  Click Link  id=obslogstab
  Sleep  1s  Waiting for tab to load
  Page Should Contain  20130711_GN-2013B-Q-60_obslog.txt
