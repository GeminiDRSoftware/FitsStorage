#include <stdio.h>
#include <unistd.h>

int main(int argc, char **argv) {
  execv("/astro/iraf/rhux-x86_64-glibc2.5/gempylocal/bin/python", argv);
  /* The execv should never return - it replaces the process image with what it calls. */
  /* if we get to the line below, something went wrong. */
  return 1;
}


