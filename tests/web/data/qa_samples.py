import datetime
from decimal import Decimal

st = datetime.datetime(2015, 7, 29, 14, 48, 25, 566795)

samples = [
    ('[{"processid": 30415, "executable": "reduce", "software_version": "GP-X2", "hostname": "mkopipe2.hi.gemini.edu", "userid": "pipeops", "qametric": [{"iq": {"comment": ["High ellipticity"], "ee50d_std": 0.0678638443350792, "elip_std": 0.06684891879558563, "fwhm": 0.5274235010147095, "ee50d": 0.6002851128578186, "fwhm_std": 0.030540257692337036, "pa_std": 50.875553131103516, "isofwhm_std": 0.05481640622019768, "pa": 21.65399169921875, "elip": 0.11259367316961288, "percentile_band": 20, "adaptive_optics": false, "nsamples": 7, "isofwhm": 0.549750804901123}, "detector": null, "datalabel": "GN-2015A-LP-1-358-001_stack", "filename": "N20150726S0205_sourcesDetected.fits"}], "context": "qa", "software": "QAP"}]',
     {'rep': {'processid': 30415, 'executable': u'reduce', 'software_version': u'GP-X2', 'hostname': u'mkopipe2.hi.gemini.edu', 'userid': u'pipeops', 'submit_host': u'localhost', 'context': u'qa', 'submit_time': st, 'software': u'QAP'},
       'iq': [{'comment': u'High ellipticity', 'nsamples': 7, 'ee50d_std': Decimal('0.068'), 'elip_std': Decimal('0.067'), 'strehl_std': None, 'fwhm': Decimal('0.527'), 'ee50d': Decimal('0.600'), 'fwhm_std': Decimal('0.031'), 'filename': u'N20150726S0205_sourcesDetected.fits', 'pa_std': Decimal('50.876'), 'isofwhm_std': Decimal('0.055'), 'pa': Decimal('21.654'), 'elip': Decimal('0.113'), 'ao_seeing': None, 'percentile_band': u'20', 'strehl': None, 'adaptive_optics': False, 'detector': None, 'datalabel': u'GN-2015A-LP-1-358-001_stack', 'isofwhm': Decimal('0.550')}],
       'zp': [],
       'sb': [],
       'pe': []
      }
    ),
    ('[{"processid": 26473, "executable": "reduce", "software_version": "GP-X2", "hostname": "mkopipe1.hi.gemini.edu", "userid": "pipeops", "qametric": [{"sb": {"comment": [], "mag_std": 0.013190875502630874, "mag": 21.683336771309467, "electrons": 472.78674420230914, "electrons_std": 5.7440021385822302, "nsamples": 149, "percentile_band": 20}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_iqMeasured.fits"}, {"sb": {"comment": [], "mag_std": 0.014782065122494225, "mag": 21.74656987296175, "electrons": 446.03819617560765, "electrons_std": 6.0727141953017014, "nsamples": 9, "percentile_band": 20}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_iqMeasured.fits"}, {"sb": {"comment": [], "mag_std": 0.017084158250022317, "mag": 21.65504434518953, "electrons": 485.26867972051912, "electrons_std": 7.6357469538114993, "nsamples": 167, "percentile_band": 20}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_iqMeasured.fits"}, {"sb": {"comment": [], "mag_std": 0.016298076204059229, "mag": 21.664310425021025, "electrons": 481.14483711231952, "electrons_std": 7.2225050487223657, "nsamples": 155, "percentile_band": 20}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_iqMeasured.fits"}, {"sb": {"comment": [], "mag_std": 0.019804429869204863, "mag": 21.680983067701707, "electrons": 473.81278249723761, "electrons_std": 8.6426076435295371, "nsamples": 133, "percentile_band": 20}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_iqMeasured.fits"}, {"sb": {"comment": [], "mag_std": 0.51742833006891098, "mag": 21.74132084350134, "electrons": 448.19980432974347, "electrons_std": 213.59817907446032, "nsamples": null, "percentile_band": 20}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_iqMeasured.fits"}], "context": "qa", "software": "QAP"}]',
     {'rep': {'processid': 26473, 'executable': u'reduce', 'software_version': u'GP-X2', 'hostname': u'mkopipe1.hi.gemini.edu', 'userid': u'pipeops', 'submit_host': u'localhost', 'context': u'qa', 'submit_time': st, 'software': u'QAP'},
       'iq': [],
       'zp': [],
       'sb': [{'comment': u'', 'nsamples': 149, 'mag_std': Decimal('0.013'), 'electrons_std': Decimal('5.74'), 'filename': u'N20150726S0263_iqMeasured.fits', 'electrons': Decimal('472.79'), 'mag': Decimal('21.683'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'percentile_band': u'20'},
              {'comment': u'', 'nsamples': 9, 'mag_std': Decimal('0.015'), 'electrons_std': Decimal('6.07'), 'filename': u'N20150726S0263_iqMeasured.fits', 'electrons': Decimal('446.04'), 'mag': Decimal('21.747'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'percentile_band': u'20'},
              {'comment': u'', 'nsamples': 167, 'mag_std': Decimal('0.017'), 'electrons_std': Decimal('7.64'), 'filename': u'N20150726S0263_iqMeasured.fits', 'electrons': Decimal('485.27'), 'mag': Decimal('21.655'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'percentile_band': u'20'},
              {'comment': u'', 'nsamples': 155, 'mag_std': Decimal('0.016'), 'electrons_std': Decimal('7.22'), 'filename': u'N20150726S0263_iqMeasured.fits', 'electrons': Decimal('481.14'), 'mag': Decimal('21.664'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'percentile_band': u'20'},
              {'comment': u'', 'nsamples': 133, 'mag_std': Decimal('0.020'), 'electrons_std': Decimal('8.64'), 'filename': u'N20150726S0263_iqMeasured.fits', 'electrons': Decimal('473.81'), 'mag': Decimal('21.681'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'percentile_band': u'20'},
              {'comment': u'', 'nsamples': None, 'mag_std': Decimal('0.517'), 'electrons_std': Decimal('213.60'), 'filename': u'N20150726S0263_iqMeasured.fits', 'electrons': Decimal('448.20'), 'mag': Decimal('21.741'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'percentile_band': u'20'}],
       'pe': []
      }
    ),
    ('[{"processid": 26473, "executable": "reduce", "software_version": "GP-X2", "hostname": "mkopipe1.hi.gemini.edu", "userid": "pipeops", "qametric": [{"detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "zp": {"comment": ["CC requirement not met at the 95% confidence level"], "photref": "SDSS8", "mag_std": 0.03634637914283273, "percentile_band": [70], "cloud_std": 0.03634637914283273, "mag": 27.2765615591077, "nsamples": 20, "cloud": 0.18440849116193903}, "filename": "N20150726S0263_refcatAdded.fits"}, {"detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "zp": {"comment": ["CC requirement not met at the 95% confidence level"], "photref": "SDSS8", "mag_std": 0.0, "percentile_band": [70], "cloud_std": 0.0, "mag": 27.26521301269531, "nsamples": 1, "cloud": 0.1957570375743316}, "filename": "N20150726S0263_refcatAdded.fits"}, {"detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "zp": {"comment": ["CC requirement not met at the 95% confidence level"], "photref": "SDSS8", "mag_std": 0.0347762601790596, "percentile_band": [70], "cloud_std": 0.0347762601790596, "mag": 27.30199671865018, "nsamples": 28, "cloud": 0.17793863836701362}, "filename": "N20150726S0263_refcatAdded.fits"}, {"detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "zp": {"comment": ["CC requirement not met at the 95% confidence level"], "photref": "SDSS8", "mag_std": 0.023575493587870165, "percentile_band": [70], "cloud_std": 0.023575493587870165, "mag": 27.33325038437058, "nsamples": 25, "cloud": 0.17576893141664485}, "filename": "N20150726S0263_refcatAdded.fits"}, {"detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "zp": {"comment": ["CC requirement not met at the 95% confidence level"], "photref": "SDSS8", "mag_std": 0.028296803097738844, "percentile_band": [70], "cloud_std": 0.028296803097738844, "mag": 27.31949269005285, "nsamples": 18, "cloud": 0.20935797532582612}, "filename": "N20150726S0263_refcatAdded.fits"}], "context": "qa", "software": "QAP"}]',
     {'rep': {'processid': 26473, 'executable': u'reduce', 'software_version': u'GP-X2', 'hostname': u'mkopipe1.hi.gemini.edu', 'userid': u'pipeops', 'submit_host': u'localhost', 'context': u'qa', 'submit_time': st, 'software': u'QAP'},
       'iq': [],
       'zp': [{'comment': u'CC requirement not met at the 95% confidence level', 'photref': u'SDSS8', 'mag_std': Decimal('0.036'), 'percentile_band': u'{70}', 'filename': u'N20150726S0263_refcatAdded.fits', 'cloud_std': Decimal('0.036'), 'nsamples': 20, 'mag': Decimal('27.277'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'cloud': Decimal('0.184')}, {'comment': u'CC requirement not met at the 95% confidence level', 'photref': u'SDSS8', 'mag_std': Decimal('0.000'), 'percentile_band': u'{70}', 'filename': u'N20150726S0263_refcatAdded.fits', 'cloud_std': Decimal('0.000'), 'nsamples': 1, 'mag': Decimal('27.265'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'cloud': Decimal('0.196')}, {'comment': u'CC requirement not met at the 95% confidence level', 'photref': u'SDSS8', 'mag_std': Decimal('0.035'), 'percentile_band': u'{70}', 'filename': u'N20150726S0263_refcatAdded.fits', 'cloud_std': Decimal('0.035'), 'nsamples': 28, 'mag': Decimal('27.302'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'cloud': Decimal('0.178')}, {'comment': u'CC requirement not met at the 95% confidence level', 'photref': u'SDSS8', 'mag_std': Decimal('0.024'), 'percentile_band': u'{70}', 'filename': u'N20150726S0263_refcatAdded.fits', 'cloud_std': Decimal('0.024'), 'nsamples': 25, 'mag': Decimal('27.333'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'cloud': Decimal('0.176')}, {'comment': u'CC requirement not met at the 95% confidence level', 'photref': u'SDSS8', 'mag_std': Decimal('0.028'), 'percentile_band': u'{70}', 'filename': u'N20150726S0263_refcatAdded.fits', 'cloud_std': Decimal('0.028'), 'nsamples': 18, 'mag': Decimal('27.319'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'cloud': Decimal('0.209')}],
       'sb': [],
       'pe': []
      }
    ),
    ('[{"processid": 26473, "executable": "reduce", "software_version": "GP-X2", "hostname": "mkopipe1.hi.gemini.edu", "userid": "pipeops", "qametric": [{"pe": {"ddec_std": 0.11722360270935746, "ddec": 0.069048998018063551, "dra": -0.48826587312760239, "nsamples": 63, "astref": "SDSS8", "dra_std": 0.18551214389915788}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_ccMeasured.fits"}, {"pe": {"ddec_std": 0.19896319429172379, "ddec": 0.12424775641299846, "dra": -0.70563542137733748, "nsamples": 4, "astref": "SDSS8", "dra_std": 0.2374407492135554}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_ccMeasured.fits"}, {"pe": {"ddec_std": 0.10895121868386075, "ddec": 0.22810647722016475, "dra": -0.28542557644236105, "nsamples": 69, "astref": "SDSS8", "dra_std": 0.1514219231621084}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_ccMeasured.fits"}, {"pe": {"ddec_std": 0.12395774635879896, "ddec": 0.15279739299656384, "dra": -0.35106644861294578, "nsamples": 74, "astref": "SDSS8", "dra_std": 0.15494922744182249}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_ccMeasured.fits"}, {"pe": {"ddec_std": 0.14335268518927063, "ddec": 0.10330447802774041, "dra": -0.47812321446706524, "nsamples": 51, "astref": "SDSS8", "dra_std": 0.11928899660102206}, "detector": "e2v 10031-23-05,10031-01-03,10031-18-04", "datalabel": "GN-2015B-Q-27-20-001", "filename": "N20150726S0263_ccMeasured.fits"}], "context": "qa", "software": "QAP"}]',
     {'rep': {'processid': 26473, 'executable': u'reduce', 'software_version': u'GP-X2', 'hostname': u'mkopipe1.hi.gemini.edu', 'userid': u'pipeops', 'submit_host': u'localhost', 'context': u'qa', 'submit_time': st, 'software': u'QAP'},
       'iq': [],
       'zp': [],
       'sb': [],
       'pe': [{'comment': None, 'ddec_std': Decimal('0.117'), 'nsamples': 63, 'filename': u'N20150726S0263_ccMeasured.fits', 'ddec': Decimal('0.069'), 'dra': Decimal('-0.488'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'astref': u'SDSS8', 'dra_std': Decimal('0.186')}, {'comment': None, 'ddec_std': Decimal('0.199'), 'nsamples': 4, 'filename': u'N20150726S0263_ccMeasured.fits', 'ddec': Decimal('0.124'), 'dra': Decimal('-0.706'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'astref': u'SDSS8', 'dra_std': Decimal('0.237')}, {'comment': None, 'ddec_std': Decimal('0.109'), 'nsamples': 69, 'filename': u'N20150726S0263_ccMeasured.fits', 'ddec': Decimal('0.228'), 'dra': Decimal('-0.285'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'astref': u'SDSS8', 'dra_std': Decimal('0.151')}, {'comment': None, 'ddec_std': Decimal('0.124'), 'nsamples': 74, 'filename': u'N20150726S0263_ccMeasured.fits', 'ddec': Decimal('0.153'), 'dra': Decimal('-0.351'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'astref': u'SDSS8', 'dra_std': Decimal('0.155')}, {'comment': None, 'ddec_std': Decimal('0.143'), 'nsamples': 51, 'filename': u'N20150726S0263_ccMeasured.fits', 'ddec': Decimal('0.103'), 'dra': Decimal('-0.478'), 'detector': u'e2v 10031-23-05,10031-01-03,10031-18-04', 'datalabel': u'GN-2015B-Q-27-20-001', 'astref': u'SDSS8', 'dra_std': Decimal('0.119')}]
     }
    )
  ]