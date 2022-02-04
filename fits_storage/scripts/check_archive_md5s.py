import sys

import requests
from datetime import datetime, timedelta

def get_archive_md5(filename):
    r = requests.get(f'https://archive.gemini.edu/jsonsummary/AnyQA/cols=CTOWEQ/includeengineering/filepre={filename}/not_site_monitoring')
    if r.status_code == 200:
        data = r.json()
        if len(data) == 0:
            print(f'{filename} not found on archive')
        else:
            if len(data) > 1:
                print(f'{filename} has multiple record on archive, using first')
            return data[0]['data_md5']
    else:
        print(f'error querying archive for {filename}')
        return None


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("useage: findmissingexports <fromdt> <todt>")
        exit()
    fromdtstring = sys.argv[1]
    todtstring = sys.argv[2]
    fromdt = datetime.strptime(fromdtstring, "%Y%m%d")
    todt = datetime.strptime(todtstring, "%Y%m%d")
    if todt < fromdt:
        fromdt, todt = todt, fromdt
    curdt = fromdt
    while curdt <= todt:
        datestring = curdt.strftime("%Y%m%d")
        print(f"{datestring}")
        r = requests.get(f'https://archive.gemini.edu/jsonsummary/{datestring}')
        if r.status_code == 200:
            data = r.json()
            for dat in data:
                filename = dat['name']
                if filename and (filename.startswith('N') or filename.startswith('S')):
                    fits_md5 = dat['data_md5']
                    archive_md5 = get_archive_md5(dat['name'])
                    if fits_md5 != archive_md5:
                        print(f'mismatched md5 seen for {filename}, fits: {fits_md5}  archive: {archive_md5}')
        curdt += timedelta(days=1)
