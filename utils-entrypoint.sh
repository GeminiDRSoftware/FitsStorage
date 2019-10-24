#!/bin/bash

# Needed for good Jenkins/Docker integration
# See akso best practices for ENTRYPOINT:
# https://docs.docker.com/develop/develop-images/dockerfile_best-practices/

set -e

exec "$@"
