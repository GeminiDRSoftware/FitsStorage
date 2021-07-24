import sys
import astrodata


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: check_association.py <filename> <cal type or \'all\'> <calfile>')
    filename = sys.argv[1]
    cals = sys.argv[2:]

    ad = astrodata.open(filename)
