*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchNIFS
  [Documentation]  Search NIFS
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIFS
  input text  name=date  20140505
  click button  name=Search
  Page Should Contain  N20140505S0111.fits

SearchNIFSDisperser
  [Documentation]  Search NIFS with Disperser
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIFS
  input text  name=date  20140505
  Select From List By Value  id:nifs_disperser  K
  click button  name=Search
  Page Should Contain  N20140505S0111.fits

SearchNIFSMask
  [Documentation]  Search NIFS with Mask
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIFS
  input text  name=date  20140505
  Select From List By Value  id:nifs_mask  3.0_Mask
  click button  name=Search
  Page Should Contain  N20140505S0110.fits

SearchNIFSExposureTime
  [Documentation]  Search NIFS with Exposure Time
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIFS
  input text  name=date  20140505
  input text  id=NIFSexpT  10
  click button  name=Search
  Page Should Contain  N20140505S0102.fits

SearchNIFSCoadds
  [Documentation]  Search NIFS with Coadds
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIFS
  input text  name=date  20140505
  input text  id=NIFScoadds  2
  click button  name=Search
  Page Should Contain  N20140505S0165.fits

SearchNIFSReadMode
  [Documentation]  Search NIFS with Read Mode
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  NIFS
  input text  name=date  20140505
  Select From List By Value  id:nifs_readmode  Medium_Object
  click button  name=Search
  Page Should Contain  N20140505S0118.fits

