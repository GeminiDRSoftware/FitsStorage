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
