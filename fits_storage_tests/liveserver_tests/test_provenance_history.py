from fits_storage_tests.liveserver_tests.helpers import fetch_helper, getserver


buzzwords = {
    'bpm_20220303_gmos-n_Ham_44_full_12amp.fits': [
        'N20220524S0004_flat.fits',
        'ce625728fb5686a73689be5c7f8a6d8',
        'create_bpm_for_gmos',
        'N20220523S0558.fits',
        'aa6abcfcb5418c2113f8b92571d9cd6a',
        'prepare',
        'N20220523S0559.fits',
        'f2a3909dbe1f7f625e65f13e40dc79',
        'N20220523S0560.fits',
        'fd13779f977866d9bbb46baddea3380c',
        'N20220523S0561.fits',
        'd7c323a125e4013b79bfe423e633',
        'N20220523S0562.fits',
        'efc60e7f48e4c36bab8cb65b22f839',
        'N20220524S0004.fits',
        'bc78777a8789d704cc033d3cbf1f49b',
        'N20220524S0005.fits',
        'ed79b9a25681c6d32a698b75158375',
        'N20220524S0006.fits',
        'eb22d8c470ee45ed23e9faa6060ece27',
        'N20220524S0007.fits',
        'a640ee35e6aa1ad1cd79ed85039309f',
        'N20220524S0008.fits',
        'N20220523S0558_bias.fits',
        'ffeea0c318843d056e1796bfa81bb43e',
        'biasCorrect',
        'addDQ',
        'addVAR',
        'overscanCorrect',
        'stackFrames',
        'writeOutputs',
        'ADUToElectrons',
        '{"suffix": "_prepared", "mdf": null, "attach_mdf": true}',
    ]
}
def test_bpm():
    server = getserver() + '/history'
    for filename in buzzwords.keys():
        fetch_helper(server, filename, buzzwords[filename])


