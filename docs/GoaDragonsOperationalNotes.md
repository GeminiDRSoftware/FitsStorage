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
