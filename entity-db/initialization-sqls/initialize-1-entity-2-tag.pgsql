CREATE TABLE Entity.Tag (
    tid     BIGINT  NOT NULL,
    name    TEXT    NOT NULL,

    -- 作用未知
    "type"              SMALLINT    NULL,
    cover_url           TEXT        NULL,
    head_cover_url      TEXT        NULL,
    description         TEXT        NULL,
    short_description   TEXT        NULL,
    create_time         TIMESTAMP   NULL,

    extras JSONB[] NULL,

    CONSTRAINT pkTag_tid PRIMARY KEY (tid)
);

CREATE PROCEDURE Entity.sp_upsertTag(
    _tid                            BIGINT,
    _name                           TEXT,
    "_type"                         SMALLINT,
    _cover_url                      TEXT,
    _head_cover_url                 TEXT,
    _description                    TEXT,
    _short_description              TEXT,
    _create_time                    TIMESTAMP,
    _extra                          JSONB
) AS $$
    DECLARE
        __extras JSONB[] := CASE _extra IS NULL WHEN true THEN ARRAY[] ELSE ARRAY[_extra] END;
    BEGIN
        INSERT INTO Tag (tid, name, "type", cover_url, head_cover_url, description, short_description, create_time, extras)
        VALUES (_tid, _name, "_type", _cover_url, _head_cover_url, _description, _short_description, _create_time, __extras)
        ON CONFLICT DO UPDATE
        SET "type" =            COALESCE("_type", "type"),
            cover_url =         COALESCE(_cover_url, cover_url),
            head_cover_url =    COALESCE(_head_cover_url, head_cover_url),
            description =       COALESCE(_description, description),
            short_description = COALESCE(_short_description, short_description),
            create_time =       COALESCE(_create_time, create_time),
            extras =            array_cat(extras, __extras)
            ;
    END $$ LANGUAGE plpgsql;
