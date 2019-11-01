*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchGNIRS
  [Documentation]  Search GNIRS
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GNIRS
  input text  name=date  20190719
  click button  name=Search
  Page Should Contain  N20190719S0011.fits

SearchGNIRSDisperser
  [Documentation]  Search GNIRS with Disperser
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GNIRS
  input text  name=date  20190719
  Select From List By Value  id:gnirs_disperser  111_mm
  click button  name=Search
  Page Should Contain  N20190719S0012.fits

SearchGNIRSCamera
  [Documentation]  Search GNIRS with Camera
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GNIRS
  input text  name=date  20190719
  Select From List By Value  id:gnirs_cam  GnirsShort
  click button  name=Search
  Page Should Contain  N20190719S0008.fits

SearchGNIRSMask
  [Documentation]  Search GNIRS with Mask
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GNIRS
  input text  name=date  20190719
  Select From List By Value  id:gnirs_mask  0.30arcsec
  click button  name=Search
  Page Should Contain  N20190719S0007.fits

SearchGNIRSFilter
  [Documentation]  Search GNIRS with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GNIRS
  input text  name=date  20190719
  Select From List By Value  id:gnirs_filter  XD
  click button  name=Search
  Page Should Contain  N20190719S0045.fits

SearchGNIRSExposureTime
  [Documentation]  Search GNIRS with Exposure Time
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GNIRS
  input text  name=date  20190719
  input text  id=GNIRSexpT  5
  click button  name=Search
  Page Should Contain  N20190719S0045.fits

SearchGNIRSWellDepth
  [Documentation]  Search GNIRS with Well Depth
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GNIRS
  input text  name=date  20190719
  Select From List By Value  id:gnirs_depth  Shallow
  click button  name=Search
  Page Should Contain  N20190719S0006.fits

SearchGNIRSReadMode
  [Documentation]  Search GNIRS with Read Mode
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GNIRS
  input text  name=date  20190719
  Select From List By Value  id:gnirs_readmode  Bright_Objects
  click button  name=Search
  Page Should Contain  N20190719S0005.fits

