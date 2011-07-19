#include <stdio.h>
#include <unistd.h>

int main(int argc, char **argv) {
  execv("/astro/iraf/rhux-x86_64-glibc2.5/gempylocal/bin/python", argv);
  return 0;
}


