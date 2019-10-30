*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchNIRI
  [Documentation]  Search NIRI
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20190701
  click button  name=Search
  Page Should Contain  N20190701S0019.fits

SearchNIRIFilter
  [Documentation]  Search NIRI with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20190701
  Select From List By Value  id:niri_filter  J
  click button  name=Search
  Page Should Contain  N20190701S0724.fits

SearchNIRIDisperser
  [Documentation]  Search NIRI with Disperser
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20120505
  Select From List By Value  id:niri_disperser  Kgrism
  click button  name=Search
  Page Should Contain  N20120505S0144.fits

SearchNIRIMask
  [Documentation]  Search NIRI with Mask
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20120505
  Select From List By Value  id:niri_mask  f6-cam
  click button  name=Search
  Page Should Contain  N20120505S0018.fits

SearchNIRICamera
  [Documentation]  Search NIRI with Camera
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20120505
  Select From List By Value  id:niri_cam  f6
  click button  name=Search
  Page Should Contain  N20120505S0025.fits

SearchNIRIExposureTime
  [Documentation]  Search NIRI with Exposure Time
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20120505
  input text  id=NIRIexpT  5
  click button  name=Search
  Page Should Contain  N20120505S0195.fits

SearchNIRICoadds
  [Documentation]  Search NIRI with Coadds
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20120505
  input text  id=NIRIcoadds  5
  click button  name=Search
  Page Should Contain  N20120505S0331.fits

SearchNIRIReadMode
  [Documentation]  Search NIRI with Read Mode
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20120505
  Select From List By Value  id:niri_readmode  Medium_Background
  click button  name=Search
  Page Should Contain  N20120505S0035.fits

SearchNIRISubarray
  [Documentation]  Search NIRI with Subarray
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIRI
  input text  name=date  20120505
  Select From List By Value  id:niri_roi  Central512
  click button  name=Search
  Page Should Contain  N20120505S0075.fits

