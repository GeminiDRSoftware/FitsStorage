"""
This module retrieves and prints out the desired values from the list created in 
orm.curation.py
"""

from fits_storage.orm import session_scope
from fits_storage.orm.header import Header
from fits_storage.orm.curation import duplicate_canonicals, duplicate_present, present_not_canonical
from optparse import OptionParser


def print_results(query, header, empty_message):
    if query.count() == 0:
        print(empty_message)
    else:
        for df_id, file in query:
            print("{header}: DiskFile id = {dfid:9}, File id = {fid:9}".format(header= header,
                                                                               dfid  = df_id,
                                                                               fid   = file.id))


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--debug", action="store_true", dest="debug", help="Increase log level to debug")
    parser.add_option("--demon", action="store_true", dest="demon", help="Run as a background demon, do not generate stdout")
    parser.add_option("--checkonly", action="store", type="string", dest="checkonly", help="Limits the search by identifying a specific substring")
    parser.add_option("--exclude", action="store", type="string", dest="exclude" , help="Limits the search by excluding    data with a specific substring")
    parser.add_option("--noeng", action="store_const", const="ENG", dest="exclude", help="Limits the search by excluding data with the ENG substring")

    (options, args) = parser.parse_args()
    checkonly = options.checkonly
    exclude = options.exclude

    # Get a database session
    with session_scope() as session:
        print_results(duplicate_canonicals(session),
                      "duplicate canonical row",
                      "No duplicate canonical rows found.")

        print_results(duplicate_present(session),
                      "duplicate present row",
                      "No duplicate present rows found.")

        print_results(present_not_canonical(session),
                      "duplicate present not canonical row",
                      "No present diskfile found to be non-canonical.")
