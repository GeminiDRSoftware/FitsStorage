#!/bin/sh
# wait-for-postgres.sh

set -e
  
host=$1
shift
cmd="$@"
  
until psql -U fitsdata -h "$host" -c "select from programpublication where id=1" fitsdata; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done
  
>&2 echo "Postgres is up - executing command"
exec $cmd
