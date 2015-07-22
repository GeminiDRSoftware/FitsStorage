# init file for package

from sqlalchemy import join, desc

from .calibration import Calibration
from .calibration_gmos import CalibrationGMOS
from .calibration_niri import CalibrationNIRI
from .calibration_gnirs import CalibrationGNIRS
from .calibration_nifs import CalibrationNIFS
from .calibration_michelle import CalibrationMICHELLE
from .calibration_f2 import CalibrationF2
from .calibration_gsaoi import CalibrationGSAOI
from .calibration_nici import CalibrationNICI
from .calibration_gpi import CalibrationGPI

from ..orm.file import File
from ..orm.diskfile import DiskFile
from ..orm.header import Header


def get_cal_object(session, filename, header=None, descriptors=None, types=None):
    """
    This function returns an appropriate calibration object for the given dataset
    Need to pass in a sqlalchemy session that should already be open, the class will not close it
    Also pass either a filename or a header object instance
    """

    # Did we get a header?
    if header == None and descriptors == None:
        # Get the header object from the filename
        query = session.query(Header).select_from(join(Header, join(DiskFile, File)))
        query = query.filter(File.name == filename).order_by(desc(DiskFile.lastmod))
        header = query.first()

    # OK, now instantiate the appropriate Calibration object and return it
    cal = None
    if header:
        instrument = header.instrument
    else:
        instrument = descriptors['instrument']


    if 'GMOS' in instrument:
        cal = CalibrationGMOS(session, header, descriptors, types)
    elif instrument == 'NIRI':
        cal = CalibrationNIRI(session, header, descriptors, types)
    elif instrument == 'GNIRS':
        cal = CalibrationGNIRS(session, header, descriptors, types)
    elif instrument == 'NIFS':
        cal = CalibrationNIFS(session, header, descriptors, types)
    elif instrument == 'michelle':
        cal = CalibrationMICHELLE(session, header, descriptors, types)
    elif instrument == 'F2':
        cal = CalibrationF2(session, header, descriptors, types)
    elif instrument == 'GSAOI':
        cal = CalibrationGSAOI(session, header, descriptors, types)
    elif instrument == 'NICI':
        cal = CalibrationNICI(session, header, descriptors, types)
    elif instrument == 'GPI':
        cal = CalibrationGPI(session, header, descriptors, types)
    else:
        cal = Calibration(session, header, descriptors, types)

    return cal
