#!/bin/bash

set -e

case $1 in
  "test")
    printf "Waiting for mysql to become available..."
    while ! mysql -h 127.0.0.1 -e 'SELECT version();' >> /dev/null ; do
    sleep 1
    done
    printf "Done.\\n"

    printf "Waiting for postgres to become available..."
    while ! nc -z 127.0.0.1 5432; do
    sleep 1
    done
    printf "Done.\\n"

    # Create secondary databases for running tests
    mysql -h 127.0.0.1 -e 'CREATE DATABASE IF NOT EXISTS sandbox2;'
    psql -h 127.0.0.1 -c 'DROP DATABASE IF EXISTS sandbox2;' -U postgres
    psql -h 127.0.0.1 -c 'CREATE DATABASE sandbox2;' -U postgres
    exec grunt test
    ;;
esac


exec "$@"