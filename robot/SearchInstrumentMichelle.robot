*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchMichelle
  [Documentation]  Search Michelle
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  michelle
  input text  name=date  20111117
  click button  name=Search
  Page Should Contain  N20111117S0104.fits

SearchMichelleFilter
  [Documentation]  Search Michelle with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  michelle
  input text  name=date  20111109
  Select From List By Value  id:michelle_filter  QBlock
  click button  name=Search
  Page Should Contain  N20111109S0165.fits

SearchMichelleDisperser
  [Documentation]  Search Michelle with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  michelle
  input text  name=date  20111109
  Select From List By Value  id:michelle_disperser  LowQ
  click button  name=Search
  Page Should Contain  N20111109S0172.fits

SearchMichelleMask
  [Documentation]  Search Michelle with Mask
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  michelle
  input text  name=date  20111109
  Select From List By Value  id:michelle_mask  4_pixels
  click button  name=Search
  Page Should Contain  N20111109S0176.fits
