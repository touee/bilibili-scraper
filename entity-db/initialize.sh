#!/usr/bin/env bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE "BilibiliScraping"
        WITH ENCODING = 'UTF8'
            LC_COLLATE = 'zh_CN.UTF-8'
            LC_CTYPE = 'en_US.UTF-8'
            CONNECTION LIMIT = -1
            TEMPLATE template0;
EOSQL

for sql_path in /tmp/initialization-sqls/{1-entity,2-assistant,3-analyzing}/*
do
    cat $sql_path \
    | psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "BilibiliScraping"
done