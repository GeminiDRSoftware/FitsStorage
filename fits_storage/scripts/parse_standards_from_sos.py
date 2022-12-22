"""
Parse SOS Standards File

This is a helper script to parse files from the SOS with new standards and convert them into the
format we expect for `standards.txt`.

Per Teo:

Attached is a zipped folder with 24 txt files, one for each field. Field names will be the same as file names.
Files contain a star name, RA, Dec, g, r, i and z mags. Let me know if you have any questions or prefer a different
star list format etc.
"""

import sys, os

from gemini_obs_db.utils.gemini_metadata_utils import ratodeg, dectodeg


def parse_line(field, line):
    line = line.strip()
    if line == '':
        return None
    fields = line.split(' ')
    if len(fields) != 7:
        print(f"Expecting 7 fields, but got [{line}]")
    star_name = fields[0]
    ra = ratodeg(fields[1])/15.0
    dec = dectodeg(fields[2])
    g = fields[3]
    r = fields[4]
    i = fields[5]
    z = fields[6]
    print(f"{star_name},{field},{ra:.6f},{dec:.5f},None,None,{g},{r},{i},{z},None,None,None,None,None,None")


if __name__ == "__main__":
    for filename in sys.argv[1:]:
        field = os.path.basename(filename)
        if '.' in field:
            field = field[:field.index('.')]
        f = open(filename, 'r')
        for line in f:
            parse_line(field, line)
