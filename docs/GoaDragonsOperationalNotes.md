# Operational Notes on DRAGONS in GOA

## Adding to Reduce Queue

`add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --tag GMOS-N_BIAS_1 --recipe checkBiasOSCO --capture_monitoring --selection /canonical/GMOS-N/BIAS/Raw/RAW/notengineering/filepre=N2010`

`add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --capture_monitoring --recipe checkFlatCounts --tag GMOS-S_IM_1 --selection /canonical/GMOS-S/dayCal/OBJECT/Raw/RAW/fullframe/imaging/slow/low/object=Twilight/filepre=S202`

# GHOST bias checking
`add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --capture_monitoring --recipe checkBiasOSCO --tag GOA-GHOST-1 --debundle INDIVIDUAL --selection /canonical/GHOST/dayCal/Raw/RAW/BIAS/filepre=S202`

in the bokeh app, then want eg:
https://archive.gemini.edu/monitoring/checkBias/BIAS/DayCal/Raw/GHOST/1x1/20251201-20251231/readspeed=red:medium,blue:slow


## Set IMQA example
`set-imqa.py --server cpofits-lv1 --selection=RAW/Raw/GMOS-S/low/slow/centralspectrum/dayCal/BIAS/filepre=S2024/UndefinedQA --pass`

# GHOST processing:

Use debundle = 'GHOST' for all 3 arms, 'GHOST-SLIT' to only process the slit arm and 'GHOST-REDBLUE' to only process the red and blue arm files.
This is needed because with the FLATs, the slit is reduced to make a slitflat, which is used in the reduction of the red/blue arm flats, so the slitflat
has to have been ingested so that it will associate when the redblue are processed. This isn't ideal as when automatically queueing files for reduction, there's
currently no automatic way to know whether the slitflat has processed, transferred and ingested before queueing the redblue etc.

## GHOST BIAS processing
add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --capture_files --debundle GHOST --tag GHOST-1 --batch ghost-bias --selection /canonical/GHOST/dayCal/BIAS/Raw/RAW/notengineering/Pass/filepre=S202512

## GHOST FLAT processing
add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --capture_files --debundle GHOST-SLIT --tag GHOST-1 --batch ghost-slitflat --after_batch ghost-bias --selection /canonical/GHOST/dayCal/FLAT/Raw/RAW/notengineering/filepre=S202512
add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --capture_files --debundle GHOST-REDBLUE --tag GHOST-1 --batch ghost-flat --after_batch ghost-slitflat --selection /canonical/GHOST/dayCal/FLAT/Raw/RAW/notengineering/filepre=S202512

## GHOST ARC processing
add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --capture_files --debundle GHOST-SLIT --tag GHOST-1 --batch ghost-slitarc --after_batch ghost-flat --selection /canonical/GHOST/dayCal/ARC/Raw/RAW/notengineering/filepre=S202512
add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --capture_files --debundle GHOST-REDBLUE --tag GHOST-1 --batch ghost-arc --after_batch ghost-slitarc --selection /canonical/GHOST/dayCal/ARC/Raw/RAW/notengineering/filepre=S202512


## GHOST science processing (Vini style)
add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --capture_files --debundle GHOST-SLIT --tag GHOST-1 --batch ghost-slit --after_batch ghost-arc --selection /canonical/GHOST/Raw/RAW/notengineering/science/filepre=S20251231S0031
add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --capture_files --capture_monitoring --debundle GHOST-REDBLUE --tag GHOST-1 --batch ghost-science --after_batch ghost-arc --uparms='{"fluxCalibrate:do_cal": "skip", "combineOrders:stacking_mode": "none"}' --selection /canonical/GHOST/Raw/RAW/notengineering/science/filepre=S20251231S0031


# Documenting what Vini's scripts do:

## calibration_reduce.py:
For one UT night:
* Download the BPMs and caldb add them

* Download all the BIAS bundles
* run reduce on all the BIAS bundles at once. This debundles them, and is equivalent to running reduce on them one at a time
* 
* run reduce on each of the resulting _slit.fits file, individually
* For each bundle file:
* * run reduce on all the _red.fits files that came from that bundle, ie reduce SYYYYMMDDSNNNN_red???.fits
* * run reduce on all the _blue.fits files that came from that bundle, ie reduce SYYYYMMDDSNNNN_red???.fits

* Download all the FLAT bundles
* run reduce on all the FLAT bundles at once, to debundle them.
* 
* run reduce on each of the resulting _slit.fits file, individually (NOTE, this is at odds with the tutorial, which reduces them all together)
* For each bundle file:
* * run reduce on all the _red.fits files that came from that bundle, ie reduce SYYYYMMDDSNNNN_red???.fits
* * run reduce on all the _blue.fits files that came from that bundle, ie reduce SYYYYMMDDSNNNN_red???.fits

* Download all the ARC bundles
* run reduce on all the ARC bundles at once, to debundle them.
* 
* run reduce on each of the resulting _slit.fits file, individually
* For each bundle file:
* * run reduce on all the _red.fits files that came from that bundle, ie reduce SYYYYMMDDSNNNN_red???.fits
* * run reduce on all the _blue.fits files that came from that bundle, ie reduce SYYYYMMDDSNNNN_red???.fits

## science_reduce.py:
For one UT night:
* Download all the science files
* run reduce on all the science bundles at once to debundle them.
* Make a list of all the resulting _slit.fits files
* run reduce on that list, all at once.
* DOES THIS MAKE SOME KIND OF NIGHTLY SLIT FILE???? I don't think this makes any difference, as it's not really stacking. Chris says he's not sure if it makes any difference, but they should not be combined.
* for each science bundle:
* * call reduce individually on each _blue???.fits file, passing -p fluxCalibrate:do_cal=skip (ie reduce ..._blue001.fits; reduce ..._blue002.fits; ...)
* * same for the _red???.fits files
* NOTE - there's no grouping at all here, not even by bundle. As far as I can see, this is equvalent to calling reduce on blue*.fits with -p combineOrders:stacking_mode=none (see the DRAGONS GHOST tutorial for info on that)
* 