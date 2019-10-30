*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchForm
  [Documentation]  Load Search Form
  Log  Test loading the search form
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  Program ID
  Page Should Contain  Search
  Log  End of test case

HelpPage
  [Documentation]  Load Help Page
  Open Browser  ${URL}/help/index.html  ${BROWSER}
  Page Should Contain  Searching for data for a particular program

AboutPage
  [Documentation]  Load About Page
  Open Browser  ${URL}/help/about.html  ${BROWSER}
  Page Should Contain  About the Gemini Observatory Archive

MiscFilesPage
  [Documentation]  Load Misc Files Page
  Open Browser  ${URL}/miscfiles  ${BROWSER}
  Page Should Contain  Generic Files Management

LoginPage
  [Documentation]  Load Login Page
  Open Browser  ${URL}/login/  ${BROWSER}
  Page Should Contain  Gemini Observatory Archive Login

RequestAccountPage
  [Documentation]  Load Request Account Page
  Open Browser  ${URL}/request_account/  ${BROWSER}
  Page Should Contain  New Account Request

SearchPICoIName
  [Documentation]  Search PI/CoI Name
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=PIname  Hirst
  input text  name=date  20130711
  click button  name=Search
  Page Should Contain  N20130711S0203.fits

LoadObsLogs
  [Documentation]  Load Associated Observation Logs
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=PIname  Hirst
  input text  name=date  20130711
  click button  name=Search
  Page Should Contain  N20130711S0203.fits
  Click Link  id=obslogstab
  Sleep  1s  Waiting for tab to load
  Page Should Contain  20130711_GN-2013B-Q-60_obslog.txt

ViewCalibrations
  [Documentation]  View Calibrations
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=PIname  Hirst
  input text  name=date  20130711
  click button  name=Search
  Page Should Contain  N20130711S0203.fits
  Click Link  id=caltab
  Sleep  3s  Waiting for tab to load
  Page Should Contain  GN-CAL20130710-22-033
