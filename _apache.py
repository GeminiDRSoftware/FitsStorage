try:
    from apache._apache import *
except ImportError:
    table = None
    log_error = None
    config_tree = None
    server_root = None
    mpm_query = None
    exists_config_define = None
    stat = None
    SERVER_RETURN = None

    AP_CONN_UNKNOWN = None
    AP_CONN_CLOSE = None
    AP_CONN_KEEPALIVE = None

    APR_NOFILE = None
    APR_REG = None
    APR_DIR = None
    APR_CHR = None
    APR_BLK = None
    APR_PIPE = None
    APR_LNK = None
    APR_SOCK = None
    APR_UNKFILE = None

    parse_qs = None
    parse_qsl = None

    MODULE_MAGIC_NUMBER_MAJOR = None
    MODULE_MAGIC_NUMBER_MINOR = None
