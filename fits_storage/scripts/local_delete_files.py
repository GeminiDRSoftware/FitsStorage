import urllib.request, urllib.parse, urllib.error
from xml.dom.minidom import parseString
import os
import re
import hashlib
import sys


def getXmlData(element, tag):
    return element.getElementsByTagName(tag)[0].childNodes[0].data


if __name__ == "__main__":

    # Option Parsing
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--tapeserver", action="store", type="string", dest="tapeserver", default="hbffitstape1", help="The Fits Storage Tape server to use to check the files are on tape")
    parser.add_option("--file-pre", action="store", type="string", dest="filepre", default='', help="File prefix to operate on, eg N20090130, N200812 etc")
    parser.add_option("--mintapes", action="store", type="int", dest="mintapes", default=2, help="Minimum number of tapes file must be on to be eligable for deletion")
    parser.add_option("--dir", action="store", type="string", dest="dir", help="Directory to operate in")
    parser.add_option("--nomd5", action="store_true", dest="nomd5", help="Do not check md5, match on filename only")
    parser.add_option("--dryrun", action="store_true", dest="dryrun", help="Dry Run - do not actually do anything")
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")

    options, args = parser.parse_args()

    if options.dir:
      os.chdir(options.dir)

    rawlist = os.listdir('.')
    restring = '^' + options.filepre + '.*'
    cre = re.compile(restring)
    thelist = list(filter(cre.match, rawlist))

    print("Files to consider: %s" % thelist)

    if options.dryrun:
        def remove(fname, md5, tapes):
            print("Dry run - not actually deleting File %s - %s which is on %d tapes: %s" % (fname, md5, len(tapes), tapes))
    else:
        def remove(fname, md5, tapes):
            print("Deleting File %s - %s which is on %d tapes: %s" % (fname, md5, len(tapes), tapes))
            try:
                os.unlink(fname)
            except:
                print("Could not unlink file %s: %s - %s" % (fname, sys.exc_info()[0], sys.exc_info()[1]))


    for thefile in thelist:
      try:
        if not os.path.isfile(thefile):
          print("%s is not a regular file - skipping" % thefile)
          continue
        m = hashlib.md5()
        block = 64*1024
        with open(thefile, 'rb') as f:
            data = f.read(block)
            m.update(data)
            while data:
              data = f.read(block)
              m.update(data)
        filemd5 = m.hexdigest()

        print("Considering %s - %s" % (thefile, filemd5))

        url = "http://%s/fileontape/%s" % (options.tapeserver, thefile)
        xml = urllib.request.urlopen(url).read()

        dom = parseString(xml)

        fileelements = dom.getElementsByTagName("file")

        tapeids = []
        for fe in fileelements:
          filename = getXmlData(fe, "filename")
          md5 = getXmlData(fe, "md5")
          tapeid = int(getXmlData(fe, "tapeid"))
          if (filename == thefile) and ((md5 == filemd5) or options.nomd5) and (tapeid not in tapeids):
            #print "Found it on tape id %d" % tapeid
            tapeids.append(tapeid)

        if len(tapeids) >= options.mintapes:
          remove(thefile, filemd5, tapeids)
        else:
          print("File %s is not on sufficient tapes to be elligable for deletion" % thefile)
      except PermissionError:
        print("No permission to process file %s: %s - %s" % (thefile, sys.exc_info()[0], sys.exc_info()[1]))
