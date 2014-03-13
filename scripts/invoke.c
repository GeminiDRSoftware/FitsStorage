#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#define PYTHON "/opt/Ureka/Ureka/variants/common/bin/python"
#define PYTHONPATH "PYTHONPATH=/opt/FitsStorage:/opt/python_modules/lib/python2.7/site-packages:/opt/gemini_python"

#define SCRIPT "/opt/FitsStorage/scripts/ingest_uploaded_file.py"

int main(int argc, char **argv) {
  extern char **environ;
  char *pythonpath=PYTHONPATH;
  char *python=PYTHON;
  char **p;

  uid_t ruid;
  uid_t euid;

  /* dump the environment completely, replace with PYTHONPATH only*/
  p = environ;
  *p = pythonpath;
  p++;
  *p = (char *)NULL;
 
  /* fix argv[0] */
  argv[0]=python;

  /* validate argv[1] */
  if (argc < 2) {
    /* no arguments */
    printf("Invoke called with no arguments\n");
    return 2;
  }
    
  if (strcmp(argv[1], SCRIPT) != 0) {
    /* not the valid script */
    printf("Invoke called with invalid script: %s\n", argv[1]);
    return 3;
  } 

  /* set the real and effective uid and replace process image */
  ruid = getuid();
  euid = geteuid();
  setreuid(euid, euid);
  execv(PYTHON, argv);

  /* The execv should never return - it replaces the process image with what it calls. */
  /* if we get to the line below, something went wrong. */
  return 1;
}
