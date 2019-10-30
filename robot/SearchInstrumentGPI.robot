*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchGPI
  [Documentation]  Search GPI
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GPI
  input text  name=date  20190912
  click button  name=Search
  Page Should Contain  S20190912S0116.fits

SearchGPIFilter
  [Documentation]  Search GPI with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GPI
  input text  name=date  20190912
  Select From List By Value  id:gpi_filter  H
  click button  name=Search
  Page Should Contain  S20190912S0121.fits

SearchGPIDisperser
  [Documentation]  Search GPI with Disperser
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GPI
  input text  name=date  20190912
  Select From List By Value  id:gpi_disperser  DISP_PRISM
  click button  name=Search
  Page Should Contain  S20190912S0121.fits

SearchGPIMask
  [Documentation]  Search GPI with Mask
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GPI
  input text  name=date  20190912
  Select From List By Value  id:GPI_mask  FPM_H
  click button  name=Search
  Page Should Contain  S20190912S0120.fits

SearchGPIApodizer
  [Documentation]  Search GPI with Apodizer
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GPI
  input text  name=date  20190912
  Select From List By Value  id:GPI_pupil  APOD_H
  click button  name=Search
  Page Should Contain  S20190912S0120.fits

