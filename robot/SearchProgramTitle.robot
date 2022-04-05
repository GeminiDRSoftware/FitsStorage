*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchProgramTitle
  [Documentation]  Search Program Title
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=date  20190619-20190621
  click button  name=Search
  Page Should Contain  N20190620S0573.fits
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=ProgramText  Exoplanet
  click button  name=Search
  Page Should Contain  N20190620S0573.fits
