import requests
payload = {'files': ['N20101122S0001.fits', 'N20101122S0002.fits']}
r = requests.post("https://archive.gemini.edu/download", data=payload)
f = open("data.tar", "w")
f.write(r.content)
f.close()
r.close()
