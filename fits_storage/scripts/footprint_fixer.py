#! /bin/python
import bz2
import os

import datetime

import numpy as np

from argparse import ArgumentParser
from sqlalchemy import join, desc, outerjoin

import astrodata
from fits_storage import fits_storage_config
from fits_storage.fits_storage_config import s3_staging_area, using_s3, z_staging_area
from fits_storage.logger import logger
from gemini_obs_db import session_scope
import sys

from gemini_obs_db.diskfile import DiskFile
from fits_storage.orm.footprint import Footprint
from fits_storage.orm.geometryhacks import add_footprint
from gemini_obs_db.header import Header
try:
    from fits_storage.utils.aws_s3 import get_helper
except:
    pass


# This is deprecated, was to fix a one off problem migrating to python3.
# Keeping for now for reference


def save_missing_footprints(filename, fromdate, todate):
    f = open(filename, "w")
    with session_scope() as session:
        headers = (
            session.query(Header).select_from(join(Header, DiskFile))
                .filter(Header.ut_datetime >= fromdate)
                .filter(Header.ut_datetime < todate)
                .filter(DiskFile.present == True)
                .order_by(desc(Header.ut_datetime))
        )

        if using_s3:
            s3helper = get_helper(logger_=logger)

        for header in headers:
            print("Checking header: %s" % header.data_label)
            fps = session.query(Footprint).filter(Footprint.header_id == header.id).all()
            if fps is None or len(fps) == 0:
                diskfile = header.diskfile
                print("Checking footprint for diskfile %s" % diskfile.filename)
                try:
                    # We munge the filenames here to avoid file collisions in the
                    # staging directories with the ingest process.
                    our_dfcc = False
                    munged_filename = diskfile.filename
                    munged_fullpath = diskfile.fullpath()
                    if using_s3:
                        # Fetch from S3 to staging area
                        # TODO: We're not checking here if the file was actually retrieved...
                        munged_filename = "preview_" + diskfile.filename
                        munged_fullpath = os.path.join(s3_staging_area, munged_filename)
                        s3helper.fetch_to_staging(diskfile.filename, fullpath=munged_fullpath)

                    if diskfile.compressed:
                        # Create the uncompressed cache filename and unzip to it
                        nonzfilename = munged_filename[:-4]
                        diskfile.uncompressed_cache_file = os.path.join(z_staging_area, nonzfilename)
                        if os.path.exists(diskfile.uncompressed_cache_file):
                            os.unlink(diskfile.uncompressed_cache_file)
                        with bz2.BZ2File(munged_fullpath, mode='rb') as in_file, open(diskfile.uncompressed_cache_file,
                                                                                      'wb') as out_file:
                            out_file.write(in_file.read())
                        our_dfcc = True
                        ad_fullpath = diskfile.uncompressed_cache_file
                    else:
                        # Just use the diskfile fullpath
                        ad_fullpath = munged_fullpath
                    # Open the astrodata instance
                    diskfile.ad_object = astrodata.open(ad_fullpath)

                    fps = header.footprints(diskfile.ad_object)
                    diskfile.ad_object = None
                    for i in list(fps.keys()):
                        footprint = fps[i]
                        if footprint is not None:
                            print("Would add footprint: %s: %s = %s" % (diskfile.filename, i, footprint))
                            f.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(diskfile.filename, i,
                                                                             footprint[0][0],
                                                                             footprint[0][1],
                                                                             footprint[1][0],
                                                                             footprint[1][1],
                                                                             footprint[2][0],
                                                                             footprint[2][1],
                                                                             footprint[3][0],
                                                                             footprint[3][1]
                                                                             ))
                        # if fps[i] is not None:
                        #     foot = Footprint(header)
                        #     foot.populate(i)
                        #     session.add(foot)
                        #     session.commit()
                        #     add_footprint(session, foot.id, fps[i])
                except Exception as e:
                    print("Footprint Exception: %s : %s..." % (sys.exc_info()[0], sys.exc_info()[1]))
                    # pass
                finally:
                    if using_s3:
                        os.unlink(munged_fullpath)
                    if our_dfcc:
                        os.unlink(ad_fullpath)
    f.flush()
    f.close()


def load_footprints(filename):
    with session_scope() as session:
        f = open(filename, "r")
        for line in f:
            fp_data = line.split('\t')
            if len(fp_data) != 10:
                print("Warning, unable to parse line: %s" % line)
            else:
                filename = fp_data[0]
                extname = fp_data[1]
                footprint = np.array([[float(fp_data[2]), float(fp_data[3])],
                                      [float(fp_data[4]), float(fp_data[5])],
                                      [float(fp_data[6]), float(fp_data[7])],
                                      [float(fp_data[8]), float(fp_data[9])]])
                hdr = session.query(Header).select_from(join(Header, DiskFile)) \
                    .filter(DiskFile.canonical == True) \
                    .filter(Header.diskfile_id == DiskFile.id)\
                    .filter(DiskFile.filename == filename).one_or_none()
                if hdr is not None and hdr.diskfile is not None:
                    fp = session.query(Footprint) \
                        .filter(Footprint.header_id == hdr.id) \
                        .filter(Footprint.extension == extname).one_or_none()
                    if fp is None:
                        #print("Would add: %s: %s: %s" % (hdr.id, extname, footprint))
                        fp = Footprint(hdr)
                        fp.populate(extname)
                        session.add(fp)
                        session.commit()
                        add_footprint(session, fp.id, footprint)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--from", action="store", dest="fromdate",
                        help="Date to search from for missing footprints")
    parser.add_argument("--to", action="store", dest="todate",
                        help="Date to search to for missing footprints")
    parser.add_argument("--save", action="store", dest="save",
                        help="File to save footprint information to as filename, extname, footprint .csv format")
    parser.add_argument("--load", action="store", dest="load",
                        help="File to load footprints from and write to database")
    args = parser.parse_args()
    if args.load and (args.fromdate or args.todate or args.save):
        print("load option should not be used with other options")
        exit(1)
    if args.save and not (args.fromdate or args.todate):
        print("save option requires from and to dates")
        exit(1)

    if args.save:
        fromdate = None
        todate = None
        if args.fromdate:
            fromdate = datetime.datetime.strptime(args.fromdate, '%Y%m%d')
        if args.todate:
            todate = datetime.datetime.strptime(args.todate, '%Y%m%d')
        save_missing_footprints(args.save, fromdate, todate)
    elif args.load:
        load_footprints(args.load)
