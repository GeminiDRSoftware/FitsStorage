import astrodata
import gemini_instruments
import numpy as np
import os

allfiles = os.listdir(".")
files = [f for f in allfiles if f.endswith("_bias.fits")]
files.sort()

reffile = files.pop()

print(f"Reference file: {reffile}")
print(f"Bias files: {files}")

refad = astrodata.open(reffile)
for f in files:
    ad=astrodata.open(f)
    s = f"{f}: "
    adiff = ad - refad
    for i in range(len(ad)):
        data = adiff[i].data
        mask = adiff[i].mask
        gooddata = data[mask == 0]
        stat = np.sqrt(np.mean(np.square(gooddata)))
        # stat = np.std(gooddata)
        s += f"{stat:10.4f} "
    print(s)
