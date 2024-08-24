from numpy import genfromtxt
from astropy.io import fits
from astropy.time import Time, TimeDelta
from datetime import datetime, timedelta
import argparse
from pathlib import Path
import glob
import pandas as pd
import numpy as np
from scipy.signal import medfilt
import logging
import sys
logger = logging.getLogger("Archive")
logger.setLevel(logging.DEBUG)

def get_datetime_from_filename(filepath):
    '''
    Extract timestamp in datetime format from MAROON-X file name.

    :param filepath: String with filename or complete path. Filename must adher to MAROON-X standard filename convention
    :return: Datetime object with timestamp taken from filename
    '''
    filename = filepath.split("/")[-1]
    return datetime.strptime(filename[:16], "%Y%m%dT%H%M%SZ")

def get_files(pattern, root, first_frame=None, last_frame=None, from_date=None, to_date=None):
    '''
    Return a list of filenames that matches a certain pattern
    :param pattern:     String with filename pattern to look for (e.g. f"*{args.date}*{args.obs_type}_b_{exptime}.fits")
    :param root:        String with root folder structure to look in for files matching pattern (e.g. /data10/MaroonX_spectra/)
    :param first_frame: String with filename of first frame to consider (check for use of pattern and from_date for other downselect options)
    :param last_frame:  String with filename of last frame to consider (check for use of pattern and to_date for other downselect options)
    :param from_date:   Datetime for start of specific time period (check for use of pattern and first_frame for other downselect options)
    :param to_date:     Datetime for end of specific time period (check for use of pattern and last_frame for other downselect options)
    :return: List of files
    '''
    files = []
    if not root.endswith("/"):
        root += "/"
    # look only in appropriate folders
    folders_to_look = []
    if first_frame is not None:
        from_date = get_datetime_from_filename(first_frame)
    elif from_date is None:
        first_directory_in_root = [subdir[-9:-1] for subdir in sorted(glob.glob(root + "/*/"))][0]
        from_date = datetime.strptime(first_directory_in_root, "%Y%m%d")
    elif from_date is not None:
        from_date = datetime.strptime(from_date, "%Y%m%d")

    if last_frame is not None:
        to_date = get_datetime_from_filename(last_frame)
    elif to_date is None:
        to_date = datetime.utcnow()
    elif to_date is not None:
        to_date = datetime.strptime(to_date, "%Y%m%d")

    delta = to_date - from_date  # timedelta
    for i in range(delta.days + 1):
        folders_to_look.append(datetime.strftime(from_date + timedelta(days=i), "%Y%m%d"))

    for folder in folders_to_look:
        for f in sorted(glob.glob(root + folder + "/" + pattern)):
            if from_date < get_datetime_from_filename(f) < to_date:
                files.append(f)

    return files

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Batch script to construct Gemini archive compatible MEF files from raw MAROON-X fits files and exposure meter datastream.',
                                     epilog="This script combines raw 2D spectra from both arms and adds exposuremeter data to form a MEF file.")

    p_input = parser.add_argument_group('Input')

    p_input.add_argument('-dd', '--data_directory', help="Directory for raw files, e.g., '/data9/MaroonX_spectra/'.",
                          type=str, required=True)

    p_input.add_argument('-d', '--date', help="UTC date of observation, e.g. '20230901' or '202309??'.",
                          type=str, default="", required=True)

    p_input.add_argument('-o', '--obs_type', help="Obs type. Default: 'SOOOE'.",
                         type=str, default="SOOOE", required=False)

    p_input.add_argument('-t', '--exptime', help="Exposure time in sec, e.g. '300'.",
                         type=str, default="*", required=False)

    p_input.add_argument('--file', help="Specify specific filename base. Ignores date and obs_type.",
                         type=str, default="", required=False)

    p_input.add_argument('-l', '--filelist', help="List of input files. Supercedes all previous search criteria. Does not check for mismatch of exposure times!",
                         type=str, default="", required=False)

    p_input.add_argument('--progid', help="Gemini ProgramID to overwrite header value, e.g. 'GN-2019B-ENG-201'.",
                         type=str, default="", required=False)

    p_input.add_argument('--overwrite',help='Overwrite exisiting file(s). Default: False',
                         action='store_true', default=False, required=False)

    p_input.add_argument('--correct_fiber',help='Correct FIBERx keyword based on filename. Default: True',
                         action='store_true', default=True, required=False)

    p_input.add_argument('-p', '--expmeter_file',
                         help="Full name and path of exposure meter file containing pandas Dataframe. "
                              "Default = '/data9/MaroonX_spectra_reduced/Maroonx_masterframes/202306xx/expmeter/expmeter062023.pkl'.",
                         type=str,
                         default="/data9/MaroonX_spectra_reduced/Maroonx_masterframes/202306xx/expmeter/expmeter062023.pkl",
                         required=False)

    p_input.add_argument('--zp_pc', help="Zeropoint for 'counts_pc' channel. Default: 6.3" ,
                         type=float, default=6.3, required=False)

    p_input.add_argument('--zp_frd', help="Zeropoint for 'counts_frd' channel. Default: 1.0" ,
                         type=float, default=1.0, required=False)


    p_output = parser.add_argument_group('Output')

    p_output.add_argument('-od', '--output_directory', help="Directory for output files. Default is the specified input data directory.",
                          type=str, default="", required=False)

    args = parser.parse_args()

    # Create log file
    fileHandler = logging.FileHandler(datetime.now().strftime('archive_%Y%m%dT%H%M%SZ.log'), mode='w')
    fileHandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)

    logger.info('COMMAND CALL: python '+' '.join(sys.argv))

    # Build list of input frames based on command line parameters.
    # Finds only blue arm frames and will then look for the associated red arm frame.
    # This script will not work for red arm files that have no blue counterpart.

    if len(args.filelist) > 0:
        logger.info(f'Use input list {args.filelist}')
        files = genfromtxt(args.filelist,dtype='str')

    elif not 'fits' in (args.file):
        if (not '*' in args.exptime) and (not '?' in args.exptime):
            exptime = args.exptime.zfill(4)
        else:
            exptime = args.exptime

        name = f"*{args.date}*{args.obs_type}_b_{exptime}.fits"

        logger.info(f'Collecting files matching pattern: {name}')

        files = sorted(get_files(name, args.data_directory))
    else:
        if '/' in args.file:
            files = [args.file]
        else:
            files = [args.data_directory + args.file]



    # Read in exposuremeter file

    exposuremeter = None
    try:
        exposuremeter = pd.read_pickle(args.expmeter_file)
    except:
        logger.warning("Could not read exposuremeter data file. No exposure meter data will be saved in FITS file.")

    for file in files:

        try:
            filename_blue = Path(file).name

            timetag = filename_blue[-34:-19]
            utc = datetime.strptime(timetag, '%Y%m%dT%H%M%S')
            utc_release = utc + timedelta(days = 365)
            timetag = utc.strftime("%Y-%m-%dT%H:%M:%S.000")
            timetag_release_1yr = utc_release.strftime("%Y-%m-%d")
            timetag_release = utc.strftime("%Y-%m-%d")

            # Define filename for red arm file associated to blue arm file

            filename_red = filename_blue[0:13]+'??'+filename_blue[15:-9]+'*.fits'
            filename_red = filename_red.replace("_b_", "_r_")


            file_blue = file
            logger.info(f'Blue arm file: {file_blue}')

            # Find red arm file associated to blue arm file

            try:
                file_red  = glob.glob(str(Path(file).parent)+'/'+filename_red)[0]
                logger.info(f'Red  arm file: {file_red}')
                filename_red = Path(file_red).name
            except:
                logger.warning('Red arm file not found')
                file_red = None

            hr  = float(filename_blue[-25:-23])
            min = float(filename_blue[-23:-21])
            sec = float(filename_blue[-21:-19])
            nnnn = round((hr*3600 + min*60 + sec)/100)

            # Define output file name for MEF

            if len(args.output_directory) > 5:
                outpath = Path(args.output_directory)
                outpath.mkdir(parents=True, exist_ok=True)
            else:
                outpath = Path(file).parent

            fitsfile_out = str(outpath)+'/N'+filename_blue[0:8]+f'M{nnnn:04d}.fits'

            # Now lets deal with updating FITS headers and save blue frame in MEF

            if file_blue is not None:

                hdu_blue = fits.open(file_blue)[0]  # open a FITS file
                hdr_blue = hdu_blue.header
                del hdr_blue['*CRYOTELRED*']
                del hdr_blue['*MAROONXRED*']
                del hdr_blue['*LASERSHUTTER*']

                if '_D' in filename_blue:
                    fiber1 = 'Dark'
                elif '_S' in filename_blue:
                    fiber1 = 'Sky'
                elif '_F' in filename_blue:
                    fiber1 = 'Flat lamp'
                elif '_E' in filename_blue:
                    fiber1 = 'Etalon'
                elif '_T' in filename_blue:
                    fiber1 = 'ThAr'
                elif '_X' in filename_blue:
                    fiber1 = 'LFC'
                elif '_L' in filename_blue:
                    fiber1 = 'LFC'
                else:
                    fiber1 = 'Unknown'

                if 'D_' in filename_blue:
                    fiber5 = 'Dark'
                elif 'F_' in filename_blue:
                    fiber5 = 'Flat lamp'
                elif 'E_' in filename_blue:
                    fiber5 = 'Etalon'
                elif 'T_' in filename_blue:
                    fiber5 = 'ThAr'
                elif 'X_' in filename_blue:
                    fiber5 = 'LFC'
                elif 'L_' in filename_blue:
                    fiber5 = 'LFC'
                elif 'I_' in filename_blue:
                    fiber5 = 'Iodine Cell'
                else:
                    fiber5 = 'Unknown'

                if 'OOO' in filename_blue:
                    # object = hdr_blue.get('HIERARCH MAROONX TELESCOPE TARGETNAME')
                    object = 'Target'
                    fiber1 = 'Sky'
                    obsclass = 'science'
                    obstype = 'OBJECT'
                elif 'DDD' in filename_blue:
                    object = 'Dark'
                    obsclass = 'dayCal'
                    obstype = 'DARK'
                    if 'DDDDE' in filename_blue and hdr_blue.get('EXPTIME') < 30:
                        obsclass = 'dayCal'
                        obstype = 'CAL'
                    if 'FDDDF' in filename_blue:
                        object = 'Flat lamp'
                        obsclass = 'dayCal'
                        obstype = 'FLAT'
                elif 'FFF' in filename_blue:
                    object = 'Flat lamp'
                    obsclass = 'dayCal'
                    obstype = 'FLAT'
                elif 'EEE' in filename_blue:
                    object = 'Etalon'
                    obsclass = 'dayCal'
                    obstype = 'CAL'
                elif 'TTT' in filename_blue:
                    object = 'ThAr'
                    obsclass = 'dayCal'
                    obstype = 'ARC'
                elif 'XXX' in filename_blue:
                    object = 'LFC'
                    obsclass = 'dayCal'
                    obstype = 'ARC'
                elif 'LLL' in filename_blue:
                    object = 'LFC'
                    obsclass = 'dayCal'
                    obstype = 'ARC'
                else:
                    object = 'Unknown'
                    obsclass = 'dayCal'
                    obstype = 'CAL'

                # If no ProgID is specified manually, use the one from the blue frame if this is a science frame (object is 'Target')
                # or create one for calibration frames based on the GC-GAL and UT date

                if not 'GN' in args.progid:
                    if 'Target' in object:
                        progid = hdr_blue.get('HIERARCH MAROONX TELESCOPE PROGRAMID')
                    else:
                        progid = 'GN-CAL' + filename_blue[-34:-26]

                # Correct FIBERx keyword (default option)

                if args.correct_fiber:
                    hdr_blue['HIERARCH FIBER1'] = fiber1
                    hdr_blue['HIERARCH FIBER2'] = object
                    hdr_blue['HIERARCH FIBER3'] = object
                    hdr_blue['HIERARCH FIBER4'] = object
                    hdr_blue['HIERARCH FIBER5'] = fiber5
                    if 'FDDDF' in filename_blue:
                        hdr_blue['HIERARCH FIBER2'] = 'Dark'
                        hdr_blue['HIERARCH FIBER3'] = 'Dark'
                        hdr_blue['HIERARCH FIBER4'] = 'Dark'

                #hdr_blue.set('EXPTIME', value=hdr_blue.get('EXPTIME'), comment='True Exposure time (sec)', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('ARM', value='BLUE', comment='Spectrograph arm', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('ORIGNAME', value = filename_blue, comment = 'Orignal file name', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('CCDTYPE', value='STA4850 (30um epi)', comment='CCD Type', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('CCDSN', value='SN26754', comment='CCD Serial Number', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('RDNOISE', value=2.9, comment='CCD Read Noise (e-/pix)', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('GAIN', value=2.72, comment='CCD Gain (e-/DN)', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('DARK', value=2.2, comment='CCD Dark Current (e-/pix/hr)', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('RDSPEED', value='100 kHz', comment='CCD Readout Speed', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('PREAMP', value='HIGH', comment='CCD Pre-Amplifier Gain', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('AMPS', value=4, comment='Number of CCD Amplifiers', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('BINNING', value='1x1', comment='CCD Binning', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('FULLWELL', value=149, comment='Full Well at 1% Linearity (ke-)', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('SCTI', value='<6.4e-7', comment='Serial Charge-Transfer-Inefficiency', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('PCTI', value='<4.7e-8', comment='Parallel Charge-Transfer-Inefficiency', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('XBIN', value=1, comment='CCD binning in X', before=hdr_blue.index('HIERARCH FIBER1'))
                hdr_blue.set('YBIN', value=1, comment='CCD binning in Y', before=hdr_blue.index('HIERARCH FIBER1'))

                hdr_blue.set('hierarch MAROONX IMAGE ORIENTATION VERTICAL FLIP', value='T', after=hdr_blue.index('HIERARCH FIBER5'))
                hdr_blue.set('hierarch MAROONX IMAGE ORIENTATION HORIZONTAL FLIP', value = 'T', after=hdr_blue.index('HIERARCH FIBER5'))

                # Create primary header for MEF

                hdr = fits.Header()
                hdr.set('TELESCOP', value = 'Gemini-North', comment = 'Name of the telescope')
                hdr.set('INSTRUME', value = 'MAROON-X', comment = 'Name of the instrument')

                hdr.set('TELESCOP', value='Gemini-North', comment='Name of telescope (Gemini-North|Gemini-South)')
                hdr.set('OBSERVAT', value='Gemini-North', comment='Observatory (Gemini-North|Gemini-South)')
                hdr.set('INSTRUME', value='MAROON-X', comment='Instrument Name')
                if 'Target' in object:
                    hdr.set('OBJECT'  , value= hdr_blue.get('HIERARCH MAROONX TELESCOPE TARGETNAME'), comment='Object Name')
                else:
                    hdr.set('OBJECT', value= object, comment='Object Name')
                hdr.set('TIMESYS' , value='UTC', comment='Time system used in this header')
                hdr.set('DATE-OBS', value=timetag, comment='UTC date time of Observation start')
                if 'science' in obsclass and 'CAL' not in progid:
                    hdr.set('RELEASE', value=timetag_release_1yr, comment='End of proprietary period YYY-MM-DD')
                else:
                    hdr.set('RELEASE', value=timetag_release, comment='End of proprietary period YYY-MM-DD')
                hdr.set('GEMPRGID', value=progid, comment='Gemini Programme ID')
                hdr.set('OBSID'   , value=progid + '-0', comment='Gemini Observation ID')
                hdr.set('DATALAB' , value=progid + '-0-0', comment='Gemini Data Label')
                hdr.set('OBSTYPE' , value=obstype.upper(), comment='Observation Type')
                hdr.set('OBSCLASS', value=obsclass, comment='Observation Class')
                hdr.set('OBSMODE' , value='SPECT', comment='Observation Mode')
                hdr.set('RA'      , value=float(hdr_blue.get('HIERARCH MAROONX TELESCOPE TELRA')) * 15.0,
                                       comment='Right Ascension (deg)')
                hdr.set('DEC'     , value=float(hdr_blue.get('HIERARCH MAROONX TELESCOPE TELDEC')),
                                       comment='Declination (deg)')
                hdr.set('AIRMASS' , value=float(hdr_blue.get('HIERARCH MAROONX TELESCOPE AIRMASS')),
                                       comment='Airmass')
                hdr.set('EXPTIME' , value=float(hdr_blue.get('EXPTIME')),
                                       comment='Exposure time (s) of blue arm')
                hdr.set('XBIN'    , value=1, comment='CCD binning in X')
                hdr.set('YBIN'    , value=1, comment='CCD binning in Y')

                hdr.set('FIBER1'  , value=fiber1, comment='Source in Fiber 1 (Sky Fiber)')
                hdr.set('FIBER2'  , value=object, comment='Source in Fiber 2 (Object Fiber)')
                hdr.set('FIBER3'  , value=object, comment='Source in Fiber 3 (Object Fiber)')
                hdr.set('FIBER4'  , value=object, comment='Source in Fiber 4 (Object Fiber)')
                hdr.set('FIBER5'  , value=fiber5, comment='Source in Fiber 5 (Sim. Calib Fiber)')
                if 'FDDDF' in filename_blue:
                    hdr.set('FIBER2', value='Dark', comment='Source in Fiber 2 (Object Fiber)')
                    hdr.set('FIBER3', value='Dark', comment='Source in Fiber 3 (Object Fiber)')
                    hdr.set('FIBER4', value='Dark', comment='Source in Fiber 4 (Object Fiber)')

                # Create primary HDU with header
                primary_hdu = fits.PrimaryHDU(header=hdr)

                # Create blue arm image extension
                image_hdu_blue = fits.ImageHDU(hdu_blue.data, name = 'SCI', header = hdr_blue,  ver=1)

                # Create HDU list from primary and blue HDU
                hdul = fits.HDUList([primary_hdu, image_hdu_blue])

                # Fix red arm file FITS header values

                if file_red is not None:

                    hdu_red = fits.open(file_red)[0]  # open a FITS file
                    hdr_red = hdu_red.header
                    del hdr_red['*CRYOTELBLUE*']
                    del hdr_red['*MAROONXBLUE*']
                    del hdr_red['*LASERSHUTTER*']

                    # Correct FIBERx keyword (default option)

                    if args.correct_fiber:
                        hdr_red['HIERARCH FIBER1'] = fiber1
                        hdr_red['HIERARCH FIBER2'] = object
                        hdr_red['HIERARCH FIBER3'] = object
                        hdr_red['HIERARCH FIBER4'] = object
                        hdr_red['HIERARCH FIBER5'] = fiber5
                        if 'FDDDF' in filename_red:
                            hdr_red['HIERARCH FIBER2'] = 'Dark'
                            hdr_red['HIERARCH FIBER3'] = 'Dark'
                            hdr_red['HIERARCH FIBER4'] = 'Dark'

                    #hdr_red.set('EXPTIME', value=hdr_red.get('EXPTIME'), comment='Exposure Time of red CCD (sec)',
                    #             before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('ARM', value='RED', comment='Spectrograph arm',
                                before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('ORIGNAME', value=filename_red, comment='Orignal file name',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('CCDTYPE', value='STA4850 (100um high-rho)', comment='CCD Type',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('CCDSN', value='SN26771', comment='CCD Serial Number',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('RDNOISE', value=3.3, comment='CCD Read Noise (e-/pix)',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('GAIN', value=2.74, comment='CCD Gain (e-/DN)',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('DARK', value=3.8, comment='CCD Dark Current (e-/pix/hr)',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('RDSPEED', value='100 kHz', comment='CCD Readout Speed',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('PREAMP', value='HIGH', comment='CCD Pre-Amplifier Gain',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('AMPS', value=2, comment='Number of CCD Amplifiers',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('BINNING', value='1x1', comment='CCD Binning',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('FULLWELL', value=153, comment='Full Well at 1% Linearity (ke-)',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('SCTI', value='<7.4e-7', comment='Serial Charge-Transfer-Inefficiency',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('PCTI', value='<7.1e-9', comment='Parallel Charge-Transfer-Inefficiency',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('XBIN', value=1, comment='CCD binning in X',
                                 before=hdr_red.index('HIERARCH FIBER1'))
                    hdr_red.set('YBIN', value=1, comment='CCD binning in Y',
                                 before=hdr_red.index('HIERARCH FIBER1'))

                    hdr_red.set('hierarch MAROONX IMAGE ORIENTATION VERTICAL FLIP', value='F',
                                 after=hdr_red.index('HIERARCH FIBER5'))
                    hdr_red.set('hierarch MAROONX IMAGE ORIENTATION HORIZONTAL FLIP', value='F',
                                 after=hdr_red.index('HIERARCH FIBER5'))

                    # Create red arm image extension
                    image_hdu_red = fits.ImageHDU(hdu_red.data, name = 'SCI', header=hdr_red, ver=2)

                    # Add red HDU to list
                    hdul.append(image_hdu_red)

                else:
                    logger.warning(f'No red arm frame found matching {filename_blue} <-------------------------------------')

                # extract exposure meter data timeseries for each file

                if exposuremeter is not None:

                    try:
                        exptime_red = hdr_red.get('EXPTIME')
                    except:
                        exptime_red = 0

                    try:
                        exptime_blue = hdr_blue.get('EXPTIME')
                    except:
                        exptime_blue = 0

                    exptime = np.max([exptime_red,exptime_blue]) # Maximum exposure time determines window for exposuremeter data.

                    dt1 = TimeDelta(exptime, format='sec')
                    dt3 = TimeDelta(300, format='sec') # Pad exposure by 300 sec on each side for zeropoint determination.

                    starttime = Time(utc, format='datetime', scale='utc')
                    endtime = starttime + dt1

                    result_pc = exposuremeter.loc[(starttime-dt3).iso[0:19]:(endtime+dt3).iso[0:19]]['counts_pc'].copy()
                    result_frd = exposuremeter.loc[(starttime-dt3).iso[0:19]:(endtime+dt3).iso[0:19]]['counts_frd'].copy()

                    if result_pc.empty or result_frd.empty:
                        logger.warning(f"Exposure not covered in exposuremeter file {args.expmeter_file}")
                        logger.warning(f"Exposuremeter file covers {exposuremeter.index.min().isoformat()[0:19]} to {exposuremeter.index.max().isoformat()[0:19]}")
                    else:
                        times_pc = Time(result_pc.index.values, format='datetime64', scale='utc')
                        readings_pc = result_pc.values.flatten()

                        median_pc = medfilt(readings_pc, 3)
                        outlier = np.where(np.abs(readings_pc - median_pc) / median_pc > 2)
                        readings_pc[outlier] = median_pc[outlier]
                        if np.sum(outlier) > 0:
                            logger.info(f'Replaced {len(outlier)} outlier value(s) in PC dataset')
                        readings_pc[readings_pc < 0] = 0.0

                        times_frd = Time(result_frd.index.values, format='datetime64', scale='utc')
                        readings_frd = result_frd.values.flatten()
                        median_frd = medfilt(readings_frd, 3)
                        outlier = np.where(np.abs(readings_frd - median_pc) / median_frd > 2)
                        readings_frd[outlier] = median_frd[outlier]
                        if np.sum(outlier) > 0:
                            logger.info(f'Replaced {len(outlier)} outlier value(s) in FRD dataset')
                        readings_frd[readings_frd < 0] = 0.0

                        col1 = fits.Column(name='Timestamp', format='23A', array=times_pc.strftime("%Y-%m-%dT%H:%M:%S.000"))
                        col2 = fits.Column(name='Flux PC Channel', format='E', array=readings_pc)
                        col3 = fits.Column(name='Flux FRD Channel', format='E', array=readings_frd)
                        table_hdu = fits.BinTableHDU.from_columns([col1, col2, col3], name='EXPOSUREMETER')
                        table_hdu.header.set('TIMESYS', value='UTC', comment='Time system used in this header')
                        table_hdu.header.set('TZERO2', value=args.zp_pc, after=table_hdu.header.index('TFORM2'))
                        table_hdu.header.set('TZERO3', value=args.zp_frd, after=table_hdu.header.index('TFORM3'))
                        table_hdu.header.set('TUNIT2', value='counts/ms', after=table_hdu.header.index('TZERO2'), comment='Flux from 0th order of the echelle')
                        table_hdu.header.set('TUNIT3', value='counts/ms', after=table_hdu.header.index('TZERO3'), comment='Flux from pupil slicer spillover')

                        hdul.append(table_hdu)

                hdul.writeto(fitsfile_out, overwrite=args.overwrite)

                logger.info(f'Wrote MEF file to: {fitsfile_out}')
                logger.info('')

        except Exception as e:
            logger.error(f'Error processing file: {file}')
            logger.error(f'Exception: {e}')

    logger.removeHandler(fileHandler)
    fileHandler.close()

    print('Done')
