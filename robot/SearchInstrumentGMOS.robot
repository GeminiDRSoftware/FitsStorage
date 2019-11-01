*** Settings ***
Library        SeleniumLibrary
Test Teardown  Close All Browsers

*** Variables ***
${URL}       https://archive.gemini.edu
${BROWSER}   Chrome

*** Test Cases ***
SearchGMOSAll
  [Documentation]  Search GMOS All
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS
  input text  name=date  20130711-20130712
  click button  name=Search
  Page Should Contain  GN2013BQ052-03.fits
  Page Should Contain  GS-2013A-Q-80

SearchGMOSAllExposureTime
  [Documentation]  Search GMOS All with Exp Time
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS
  input text  name=date  20130712
  input text  id=GMOSexpT  4
  click button  name=Search
  Page Should Contain  S20130712S0200.fits

SearchGMOSAllBinning
  [Documentation]  Search GMOS All with Binning
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS
  input text  name=date  20130712
  Select From List By Value  id:gmos_binning  2x2
  click button  name=Search
  Page Should Contain  S20130712S0113.fits

SearchGMOSAllDisperser
  [Documentation]  Search GMOS All with Disperser
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS
  input text  name=date  20130712
  Select From List By Value  id:gmos_disperser  R400
  click button  name=Search
  Page Should Contain  S20130712S0118.fits

SearchGMOSAllFilter
  [Documentation]  Search GMOS All with Filter
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS
  input text  name=date  20130712
  Select From List By Value  id:gmos_filter  g
  click button  name=Search
  Page Should Contain  S20130712S0102.fits

SearchGMOSAllMask
  [Documentation]  Search GMOS All with Mask
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS
  input text  name=date  20130712
  Select From List By Value  id:gmos_mask  1.0arcsec
  click button  name=Search
  Page Should Contain  S20130712S0100.fits

SearchGMOSAllNodShuffle
  [Documentation]  Search GMOS All with Nod & Shuffle
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS
  input text  name=date  20130711-20130712
  Select From List By Value  id:NandS  Classic
  click button  name=Search
  Page Should Contain  S20130712S0078.fits

SearchGMOSAllReadoutSpeed
  [Documentation]  Search GMOS All Readout Speed
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS
  input text  name=date  20130711-20130712
  Select From List By Value  id:gmos_readspeed  fast
  click button  name=Search
  Page Should Contain  N20130712S0005.fits

SearchGMOSAllReadoutGain
  [Documentation]  Search GMOS All Readout Gain
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS
  input text  name=date  20130711-20130712
  Select From List By Value  id:gmos_gain  low
  click button  name=Search
  Page Should Contain  S20130712S0088.fits

SearchGMOSAllROI
  [Documentation]  Search GMOS All ROI
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS
  input text  name=date  20130712
  Select From List By Value  id:gmos_roi  Central Spectrum
  click button  name=Search
  Page Should Contain  GS20130712S0238_BIAS.fits

SearchGMOSN
  [Documentation]  Search GMOS North
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS-N
  input text  name=date  20130712
  click button  name=Search
  Page Should Contain  N20130712S0134.fits

SearchGMOSS
  [Documentation]  Search GMOS South
  Open Browser  ${URL}/searchform  ${BROWSER}
  Page Should Contain  PI/CoI Name
  Select From List By Value  id:instselect  GMOS-S
  input text  name=date  20130712
  click button  name=Search
  Page Should Contain  S20130712S0094.fits

