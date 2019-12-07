FROM postgres:12

RUN localedef -i zh_CN -c -f UTF-8 -A /usr/share/locale/locale.alias zh_CN.UTF-8
# ENV LANG zh_CN.utf8

# ENV POSTGRES_PASSWORD_FILE=/run/secrets/BILIBILI_SCRAPING__ENTITY_DB__POSTGRES_PASSWORD

COPY ./initialize.sh /docker-entrypoint-initdb.d/
COPY ./initialization-sqls/ /tmp/initialization-sqls