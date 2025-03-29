#! /usr/bin/env python3

from optparse import OptionParser
import re
import json
from astropy.io import fits

from fits_storage.logger import logger, setdebug, setdemon

"""
Helper script that modifies fits headers in a convenient way.

Files to modify can be specified individually, or in a list file (plain
text one per line).

All actions will be invoked for all files. You can specify multiple actions,
and they will all be invoked in the order given, in one pass per file for 
efficiency.

"""


def main():
    usage = "usage: %prog [options] filename..."
    parser = OptionParser(usage)
    parser.add_option("--listfile", action="store", dest="listfile",
                      help="Text file containing list of FITS file to modify."
                           "One per line.")
    parser.add_option("--action", action="append", dest="actions",
                      help="Action(s) to be taken on each fits file. "
                           "This can be given more than once to do multiple "
                           "header updates in the same pass. The argument is"
                           "a formatted string as follows: "
                           "ADD:KEYWORD=VALUE - add KEYWORD with VALUE "
                           "SET:KEYWORD=VALUE - set existing KEYWORD to VALUE "
                           "UPDATE:KEYWORD=OLD=NEW - update KEYWORD to value "
                           "NEW only if it currently has the value OLD. "
                           "There are some restrictions on formatting imposed "
                           "by parsing this on the command line, including only"
                           "being able to update headers in the PHU. See "
                           "actionfile if you need more versatility.")
    parser.add_option("--actionfile", action="store", dest="actionfile",
                      help="A JSON formatted file, consisting a list of"
                           "dictionaries specifying actions to take on each "
                           "file. Each dictionary should contain the keys "
                           "'action' = 'ADD|SET|UPDATE', 'keyword' = the fits"
                           "keyword, 'new_value' = the new value to take, "
                           "'old_value' = the old value, only for update "
                           "actions, and 'comment' being the FITS header"
                           "comment if you want to set it. You can also specify"
                           " 'hdu' = 'PHU' (the default, to update the PHU only"
                           "), 'ALL' (to update ALL HDUs) or an integer (to "
                           "specify an individual HDU to update. Instead of "
                           "'new_value' you can specify 'from_keyword' which"
                           "will get the new value from an existing keyword.")
    parser.add_option("--dryrun", action="store_true", dest="dryrun",
                      help="Do not actually modify files, but run through the "
                           "files and report what would actually be done")
    parser.add_option("--parseonly", action="store_true", dest="parseonly",
                      help="Parse the command line options only. Use --debug"
                           "to see the parsed actions")
    parser.add_option("--backup", action="store_true", dest="backup",
                      help="Create .bak backup files of any files modified")
    parser.add_option("--debug", action="store_true", dest="debug")
    parser.add_option("--demon", action="store_true", dest="demon")

    (options, args) = parser.parse_args()

    # Handle logging options
    setdebug(options.debug)
    setdemon(options.demon)

    # Validate filename options and build list of filenames
    filenames = []
    if len(args) > 0:
        filenames = args
        logger.info("Processing %d files from command line", len(filenames))
        if options.listfile:
            logger.error("Both listfile and command line filenames specified. "
                         "Exiting out of an abundance of caution.")
            return
    elif options.listfile:
        logger.debug("Reading list of files from %s", options.listfile)
        try:
            with open(options.listfile, "r") as file:
                while line := file.readline():
                    line = line.strip().rstrip(',')
                    if line[0] != '#':
                        filenames.append(line)
        except FileNotFoundError:
            logger.error("List File not found: %s", options.listfile)
            return
        except PermissionError:
            logger.error("Permission error reading: %s", options.listfile)
            return
        except Exception:
            logger.error("Error reading list file %s", options.listfile,
                         exc_info=True)
            return

        logger.info("Read %d files from listfile: %s",
                    len(filenames), options.listfile)
        if len(filenames) == 0:
            logger.error("No files to process. Exiting.")
            return

    # Parse action list and build action dictionaries.
    if options.actionfile:
        logger.debug("Reading actions from %s", options.actionfile)
        try:
            with open(options.actionfile, "r") as file:
                actions = json.load(file)
        except FileNotFoundError:
            logger.error("Action File not found: %s", options.actionfile)
            return
        except PermissionError:
            logger.error("Permission error reading: %s", options.actionfile)
            return
        except Exception:
            logger.error("Error reading action file %s", options.actionfile,
                         exc_info=True)
            return
        except json.JSONDecodeError:
            logger.error("Error Decoding JSON in %s", options.actionfile,
                         exc_info=True)
            return

        logger.info("Read %d actions from actionfile: %s",
                    len(actions), options.actionfile)
        for action in actions:
            logger.debug("Action: %s", action)

    elif options.actions:
        # Actions is a list of dictionaries, of the form:
        # {action: add|set|update,
        # keyword: FITS keyword,
        #  new_value: new keyword value
        #  old_value: old keyword value (only for modify)}
        actions = []
        cre = re.compile(r"(?P<act>ADD:|SET:|UPDATE:)"
                         r"(?P<key>[A-Z0-9_]+)="
                         r"(?P<a>[\w+.-]+)(?:=(?P<b>[\w+.-]+))?")
        for string in options.actions:
            if m := cre.match(string):
                action = {'action': m['act'].rstrip(':'),
                          'keyword': m['key']}
                if m['act'] == 'UPDATE:':
                    if m['b'] is None:
                        logger.warning("Invalid update action - requires old "
                                       "and new values. Aborting.")
                        return
                    action['old_value'] = m['a']
                    action['new_value'] = m['b']
                else:
                    action['new_value'] = m['a']
                logger.debug("Parsed action: %s", action)
                actions.append(action)
            else:
                logger.error("Could not parse action string: %s. Exiting.",
                             string)
                return
    else:
        logger.error("No actions or actionfile specified. Exiting")
        return

    if options.parseonly:
        logger.debug("--parseonly option given, exiting now")
        return

    for filename in filenames:
        modify_fitsfile(filename, actions, options, logger)


def modify_fitsfile(filename, actions, options, logger):
    mode = 'readonly' if options.dryrun else 'update'
    save_backup = True if options.backup else False
    logger.info("Opening FITS file %s (mode %s)", filename, mode)
    hdulist = fits.open(filename, mode=mode, save_backup=save_backup,
                        do_not_scale_image_data=True)

    for action in actions:
        apply_action(hdulist, action, logger)

    logger.debug("Closing FITS file")
    hdulist.close()


def apply_action(hdulist, actdict, logger):
    logger.debug("- Applying action %s", actdict)
    action = actdict.get('action')
    keyword = actdict.get('keyword')
    old_value = actdict.get('old_value')
    new_value = actdict.get('new_value')
    from_keyword = actdict.get('from_keyword')
    comment = actdict.get('comment')
    hdu = actdict.get('hdu', 0)

    # Parse hdu and generate headerlist which is a list of header objects to
    # apply the action to. We sneak an extra 'hdunum' item into the header
    # object so that we can identify it later
    if hdu == 'PHU':
        hdu = 0
    headerlist = []
    if hdu == 'ALL':
        for i in range(len(hdulist)):
            hdulist[i].header.hdunum = i
            headerlist.append(hdulist[i].header)
    elif isinstance(hdu, int):
        if hdu in range(len(hdulist)):
            hdulist[hdu].header.hdunum = hdu
            headerlist.append(hdulist[hdu].header)
        else:
            logger.warning("File does not contain requested HDU %d, skipping",
                           hdu)
    else:
        logger.error("Invalid HDU specified in action: %s", hdu)

    # Loop through the headerlist
    for header in headerlist:
        logger.debug('Updating hdunum %d ', header.hdunum)

        kw_exists = keyword in header
        current_value = header.get(keyword)

        if from_keyword is not None:
            if from_keyword not in header:
                logger.warning(f"from_keyword keyword {keyword} is not "
                               f"present in header. Skipping action")
                continue
            else:
                new_value = header[from_keyword]

        if action not in ['ADD', 'SET', 'UPDATE']:
            logger.error("Invalid action. This should NOT happen")
            return

        if action == 'ADD':
            if kw_exists:
                logger.warning("Keyword to add (%s) already exists in header. "
                               "Skipping add action", keyword)
                continue

        elif action == 'SET':
            if not kw_exists:
                logger.warning("Keyword to set (%s) does not exist in header. "
                               "Skipping set action", keyword)
                continue

        elif action == 'UPDATE':
            if not kw_exists:
                logger.warning("Keyword to update (%s) does not exist in "
                               "header. Skipping update action", keyword)
                continue
            if old_value != current_value:
                logger.debug("Keyword to update (%s) has current value %s which"
                             "differs from old value given %s - not updating",
                             keyword, current_value, old_value)
                continue

        if comment:
            logger.debug("Assigning header %s with value %s and comment %s",
                         keyword, new_value, comment)
            header[keyword] = (new_value, comment)
        else:
            logger.debug("Assigning header %s with value %s",
                         keyword, new_value)
            header[keyword] = new_value


if __name__ == "__main__":
    main()
