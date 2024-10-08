import requests


if __name__ == "__main__":

    url = "http://fits/qaforgui"
    r = requests.get(url)
    data = r.json()

    for i in data:
        instrument = i['metadata']['instrument']
        filter = i['metadata']['filter']
        raw_filename = i['metadata']['raw_filename']

        # Does this qametric report contain an IQ measurement?
        if('iq' in list(i.keys())):
            zenith_iq = i['iq']['zenith']
            zenith_iq_err = i['iq']['zenith_error']

            print("%s %s band from %s : %.2f +- %.2f" % (instrument, filter, raw_filename, zenith_iq, zenith_iq_err))
