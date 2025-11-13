# Operational Notes on DRAGONS in GOA

## Adding to Reduce Queue

`add_to_reduce_queue.py --selection /canonical/filepre=N201/GMOS-N/BIAS/Raw/RAW/notengineering --initiatedby phirst --intent Quick-Look --tag GMOSCALDEV --recipe checkBias1 --capture_monitoring`

`add_to_reduce_queue.py --initiatedby phirst --intent Science-Quality --capture_monitoring --recipe checkFlatCounts --tag GMOS-S_IM_1 --selection /canonical/GMOS-S/dayCal/OBJECT/Raw/RAW/fullframe/imaging/slow/low/object=Twilight/filepre=S202`

## Set IMQA example
`set-imqa.py --server cpofits-lv1 --selection=RAW/Raw/GMOS-S/low/slow/centralspectrum/dayCal/BIAS/filepre=S2024/UndefinedQA --pass`
