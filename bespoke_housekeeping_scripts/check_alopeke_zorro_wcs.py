import os

import astropy.io.fits as pf

from gemini_obs_db.utils.hashes import md5sum


def check_file(inpath, filename, fix=False):
    fullpath = os.path.join(inpath, filename)
    if fix:
        fixpath = os.path.join(fix, filename)
        if os.path.exists(fixpath):
            print("%s already exists" % fixpath)
            return False
    hl = pf.open(fullpath, do_not_scale_image_data=True, mode='readonly')
    header = hl[0].header
    good = None
    if 'CTYPE1' not in header or 'CTYPE2' not in header:
        return False
    if header['CTYPE1'] == 'RA---TAN' or header['CTYPE2'] == 'RA---TAN':
        return True
    if header['CTYPE1'] == 'RA--TAN':
        good = False
        if fix:
            header['CTYPE1'] = 'RA---TAN'
    if header['CTYPE2'] == 'RA--TAN':
        good = False
        if fix:
            header['CTYPE2'] = 'RA---TAN'
    if not good and fix:
        outfile = os.path.join(fix, filename)
        hl.writeto(outfile, output_verify='silentfix+exception')
    hl.close()
    return good


if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--source", action="store", type="string", dest="source", help="source folder for input files")
    parser.add_option("--good", action="store", type="string", dest="good", help="destination for good ones")
    parser.add_option("--bad", action="store", type="string", dest="bad", help="destination for bad ones")
    parser.add_option("--fix", action="store", type="string", dest="fix", help="destination for fixed ones. Omit to test only")
    parser.add_option("--pre", action="store", type="string", dest="pre", help="file prefix to work on")

    (options, args) = parser.parse_args()

    if None in [options.source, options.good, options.bad]:
        print("Need to give source, good, bad");
        exit(1)

    for filename in os.listdir(options.source):
        if options.pre and not filename.startswith(options.pre):
            continue
        if options.fix:
            if check_file(options.source, filename, fix=options.fix):
                print("%s is good" % filename)
                outfile = os.path.join(options.good, filename)
                infile = os.path.join(options.source, filename)
                os.rename(infile, outfile)
            else:
                print("%s was fixed" % filename)
        else:
            if check_file(options.source, filename, fix=False):
                print("%s is good" % filename)
                outfile = os.path.join(options.good, filename)
            else:
                print("%s is bad" % filename)
                outfile = os.path.join(options.bad, filename)
            os.rename(fullpath, outfile)

