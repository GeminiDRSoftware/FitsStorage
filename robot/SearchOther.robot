*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchObsClassDayCal
  [Documentation]  Search Observation Class dayCal
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=date  20130712
  Select From List By Value  name:observation_class  dayCal
  click button  name=Search
  Page Should Contain  N20130712S0116.fits

SearchObsTypeMosMask
  [Documentation]  Search Observation Type MOS MASK
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=date  20130712
  Select From List By Value  name:observation_type  MASK
  click button  name=Search
  Page Should Contain  GN2013BQ052-04.fits

SearchModeLongSlitSpec
  [Documentation]  Search Mode Long Slit Spec
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=date  20130712
  Select From List By Value  name:mode  LS
  click button  name=Search
  Page Should Contain  S20130712S0118.fits

SearchAdaptiveOpticsAO
  [Documentation]  Search Adaptive Optics AO
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=date  20130712
  Select From List By Value  name:AO  AO
  click button  name=Search
  Page Should Contain  N20130712S0041.fits

SearchProgramTitle
  [Documentation]  Search Program Title
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=ProgramText  Exoplanet
  click button  name=Search
  Page Should Contain  N20190620S0573.fits

SearchTargetName
  [Documentation]  Search Adaptive Optics AO
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  input text  name=date  20120629
  input text  name=object  m101
  click button  name=Search
  Page Should Contain  N20120629S0179.fits

