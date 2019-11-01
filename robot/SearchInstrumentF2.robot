*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchF2
  [Documentation]  Search F2
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  F2
  input text  name=date  20150501
  click button  name=Search
  Page Should Contain  S20150501S0127.fits

SearchF2Filter
  [Documentation]  Search F2 with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  F2
  input text  name=date  20150501
  Select From List By Value  id:F2_filter  Ks
  click button  name=Search
  Page Should Contain  S20150501S0124.fits

SearchF2Disperser
  [Documentation]  Search F2 with Disperser
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  F2
  input text  name=date  20150501
  Select From List By Value  id:F2_disperser  JH
  click button  name=Search
  Page Should Contain  S20150501S0100.fits

SearchF2Mask
  [Documentation]  Search F2 with Mask
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  F2
  input text  name=date  20150501
  Select From List By Value  id:F2_mask  6pix-slit
  click button  name=Search
  Page Should Contain  S20150501S0128.fits

SearchF2ExposureTime
  [Documentation]  Search F2 with Exposure Time
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  F2
  input text  name=date  20150501
  input text  id=F2expT  3
  click button  name=Search
  Page Should Contain  S20150501S0446.fits

