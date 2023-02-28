from .gmos import Gmos
from .gnirs import Gnirs
from .gsaoi import Gsaoi
from .gpi import Gpi
from .niri import Niri
from .nici import Nici
from .nifs import Nifs
from .michelle import Michelle
from .f2 import F2
from .ghost import Ghost

instrument_class = {
    # Instrument: Class
    'F2': F2,
    'GMOS-N': Gmos,
    'GMOS-S': Gmos,
    'GHOST': Ghost,
    'GNIRS': Gnirs,
    'GPI': Gpi,
    'GSAOI': Gsaoi,
    'michelle': Michelle,
    'NICI': Nici,
    'NIFS': Nifs,
    'NIRI': Niri,
    }
