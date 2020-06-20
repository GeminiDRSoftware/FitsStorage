# Wikiwiki File Limit Testing Procedure

## 2007-08-31. DRAFT


This document outlines the procedure used in testing the file number limit on the summit Netapp machine wikiwiki which took place on 2007-08-28.  

1. After disabling the datamanager, the /dataflow directory was filled with empty (0 byte) files until the limit was reached.  This was accomplished via a small Python script which would generate a specified number of empty files.  The limit was found to be 139,070 files.

2. Several subdirectories were then created within the /dataflow directory and were also filled until they hit their file limit.  The limit in both of these subdirectories was found to be 4478 files.

3. Within these subdirectories, several more subdirectory layers were created down to three levels below the /dataflow directory (/dataflow/level1/level2/level3).  All subdirectories, no matter their level, were found to have the same file limit of 4478 files.

4. To test the possible dependency of the subdirectory file limit on the number of files in the top-level /dataflow directory, all empty files that had been created in the /dataflow directory were removed and  attempts where made to add more files to subdirectories at various levels.  All were unsuccessful due to the file limit.  This seems to indicate that the /dataflow and subdirectory limits are not dependent on each other.

5. Testing was also done on whether files could be moved from /dataflow into a subdirectory and how/if this would affect the file limit(s).  All test subdirectories of /dataflow were removed and the directory was again filled with empty files.  A subdirectory was created and files were moved into it, but the limit of 4478 files was reached and no more files could be moved.

6. The dataflow directory was then restored to its previous state and the datamanager was re-enabled.

