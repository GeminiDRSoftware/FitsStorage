#! /usr/bin/env python3

from argparse import ArgumentParser
import requests
from os import path, makedirs
import hashlib


def fetch_files_by_selection(server, selection,
                             check=True, dryrun=False, debug=False):
    if not server:
        print("No server specified, exiting")
        exit(1)
    if not selection:
        print("No selection specified, exiting")
        exit(2)

    # Get the file list
    filelist = get_filelist(server, selection,
                            debug=debug)
    print(f"Got {len(filelist)} files")

    downloaded = 0
    failed = 0
    skipped = 0
    for entry in filelist:
        if already_downloaded(entry, debug):
            print(f"Skipping {entry['filename']}")
            skipped += 1
        else:
            ok = download_file(server, entry, dryrun, debug)
            if ok:
                downloaded += 1
            else:
                failed += 1
    print(f"Downloaded {downloaded}, and skipped {skipped} files")
    if failed or debug:
        print(f"{failed} files failed to download")



def get_filelist(server, selection, debug=False):
    url = f"{server}/jsonfilelist/{selection}"
    if debug:
        print(f"Fetching file list from {url}")
    filelist = requests.get(url).json()
    return filelist

def already_downloaded(entry, debug=False):
    pfn = path.join(entry['path'], entry['filename'])
    if not path.isfile(pfn):
        if debug:
            print(f"File {pfn} does not exist or is not a file")
        return False
    if not path.getsize(pfn) == entry['file_size']:
        if debug:
            print(f"File {pfn} exists but is wrong size")
        return False
    # Check the md5
    if getmd5(pfn) != entry['file_md5']:
        if debug:
            print(f"File {pfn} exists but md5 is wrong")
        return False
    return True

def download_file(server, entry, dryrun=False, debug=False):
    url = '/'.join([server, 'file', entry['path'], entry['filename']])
    if dryrun:
        print(f"Dryrun - not downloading from: {url}")
        return False
    if debug:
        print(f"Download URL is: {url}")

    if entry['path']:
        makedirs(entry['path'], exist_ok=True)

    pfn = path.join(entry['path'], entry['filename'])

    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(pfn, 'wb') as f:
                for chunk in r.iter_content(chunk_size=None):
                    f.write(chunk)
    except requests.HTTPError:
        print(f"HTTP error fetching {url}")
        if debug:
            raise

    # Check md5 of downloaded file
    if getmd5(pfn) != entry['file_md5']:
        print(f"Downloaded file has wrong md5!")
        return False

    return True

def getmd5(fn):
    with open(fn, 'rb') as f:
        digest = hashlib.file_digest(f, "md5")
    return digest.hexdigest()

if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument("--debug", action="store_true", dest="debug",
                        help="Increase log level to debug")

    parser.add_argument("--dryrun", action="store_true", dest="dryrun",
                        help="Do everything except the actual download")

    parser.add_argument("--selection", action="store", type=str, default=None,
                        help="URL-style selection criteria.")

    parser.add_argument("--check_local", action="store_false",
                        help="Check if the file exists with the correct md5 "
                             "locally already and skip download if so. "
                             "Default is True")

    parser.add_argument("--server", action="store", type=str,
                        help="Server to use. Default is https://archive.gemini.edu",
                        default="https://archive.gemini.edu")

    options = parser.parse_args()

    fetch_files_by_selection(options.server, options.selection,
                             check=options.check_local,
                             dryrun=options.dryrun,
                             debug=options.debug)
