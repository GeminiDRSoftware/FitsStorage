#!/usr/bin/env python3

import argparse
import requests

parser = argparse.ArgumentParser(description='Update release data on a '
                                             'selection of files, using fits '
                                             'server APIs')
parser.add_argument('--server', type=str, help="fits server to call")
parser.add_argument('--selection', type=str, help="selection/criteria")
parser.add_argument('--dryrun', action='store_true', help="Don't actually do")
parser.add_argument('--apicookie', type=str, help="API cookie")
args = parser.parse_args()

# Get the file list from the server
url = f"{args.server}/jsonsummary/present/{args.selection}"
r = requests.get(url)
files = r.json()

print(f"Got {len(files)} to process...")

update_payload = []

for file in files:
    try:
        date = file['ut_datetime'][:10]
    except:
        print(f"Could not parse datetime for filename {file['filename']}")
        continue

    print(f"Processing {file['filename']}")

    update_dict = {'filename': file['filename'], 'values': {'release': date}}

    update_payload.append(update_dict)

print(f"Got {len(update_payload)} updates to post...")

cookies = {'gemini_api_authorization': args.apicookie}
url = f"{args.server}/update_headers"
if args.dryrun:
    print("--dryrun, not proceeding further")
else:
    r = requests.post(url, json=update_payload, cookies=cookies)
    print(f"update_headers response code: {r.status_code}")
    print(f"update_headers response text: {r.text}")

print("Done")
