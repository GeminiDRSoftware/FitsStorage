import os
from FitsStorageXmlClientUtils import get_file_list, fetch_files


if __name__ == "__main__":

    # The basic Data selection. For example:
    base_selection = "/GMOS/Imaging/OBJECT/20120610-20120625"

    # The elevation bins
    els = ['30:50', '50:70', '70:90']

    # The CRPA bins
    crpas = ['0:90', '90:180', '180:270', '270:360']

    # Get the current working directory
    cwd = os.getcwd()

    # Loop over each crpa at each elevation
    for el in els:
      for crpa in crpas:
        # Process this elevation and crpa bin

        # Make the selection for this bin
        selection = base_selection + '/el=%s/crpa=%s' % (el, crpa)

        # Make a directory name for this bin. Replace the :s with _s for nicer filenames

        dirname = "el_%s_crpa_%s" % (el.replace(':', '_'), crpa.replace(':', '_'))
        if(os.path.exists(dirname)):
           print("%s already exists, skipping")
        else:
           print("Creating %s" % dirname)
           os.chdir(cwd)
           os.mkdir(dirname)
           os.chdir(dirname)

           files = get_file_list(selection)

           numfiles = len(files)
           print("Got %d files\n" % numfiles)

           # Fetch the files
           fetch_files(files)

    os.chdir(cwd)
