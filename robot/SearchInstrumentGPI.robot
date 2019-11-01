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
  input text  name=date  20190810-20190811
  click button  name=Search
  Page Should Contain  S20190811S0226.fits

SearchGPIFilter
  [Documentation]  Search GPI with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GPI
  input text  name=date  20190810-20190811
  Select From List By Value  id:gpi_filter  H
  click button  name=Search
  Page Should Contain  S20190811S0227.fits

SearchGPIDisperser
  [Documentation]  Search GPI with Disperser
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GPI
  input text  name=date  20190810-20190811
  Select From List By Value  id:gpi_disperser  DISP_PRISM
  click button  name=Search
  Page Should Contain  S20190811S0227.fits

SearchGPIMask
  [Documentation]  Search GPI with Mask
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GPI
  input text  name=date  20190811
  Select From List By Value  id:GPI_mask  FPM_H
  click button  name=Search
  Page Should Contain  S20190811S0862.fits

SearchGPIApodizer
  [Documentation]  Search GPI with Apodizer
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GPI
  input text  name=date  20190811
  Select From List By Value  id:GPI_pupil  APOD_H
  click button  name=Search
  Page Should Contain  S20190812S0005.fits

