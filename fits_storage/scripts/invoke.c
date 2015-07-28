#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#define PYTHON "/opt/Ureka/Ureka/variants/common/bin/python"
#define PYTHONPATH "PYTHONPATH=/opt/FitsStorage/fits_storage:/opt/python_modules/lib/python2.7/site-packages:/opt/gemini_python"
#define UR_DIR "UR_DIR=/opt/Ureka/Ureka"
#define UR_VARIANT "UR_VARIANT=common"
#define UR_OS "UR_OS=linux-rhe6"
#define UR_CPU "UR_CPU=x86_64"
#define UR_BITS "UR_BITS=64"

#define SCRIPT "/opt/FitsStorage/fits_storage/scripts/ingest_uploaded_file.py"

int main(int argc, char **argv) {
  extern char **environ;
  char *python=PYTHON;
  char *pythonpath=PYTHONPATH;
  char *urdir=UR_DIR;
  char *urvariant = UR_VARIANT;
  char *uros = UR_OS;
  char *urcpu = UR_CPU;
  char *urbits = UR_BITS;
  char **p;

  uid_t ruid;
  uid_t euid;

  /* dump the environment completely, replace with PYTHONPATH and stuff Ureka needs only*/
  p = environ;
  *p++ = pythonpath;
  *p++ = urdir;
  *p++ = urvariant;
  *p++ = uros;
  *p++ = urcpu;
  *p++ = urbits;
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
