CREATE TABLE "Entity"."User" (
    uid     BIGINT  NOT NULL,
    name    TEXT    NOT NULL,

    avatar_url                  TEXT    NULL,
    hides_folders               BOOLEAN NULL,
    -- 来自 space/archive 的结果
    current_visible_video_count INTEGER NULL,

    extras JSONB[] NULL,

    CONSTRAINT "pkUser_uid" PRIMARY KEY (uid)
);

CREATE PROCEDURE "Entity"."sp_upsertUser" (
    _uid                            BIGINT,
    _name                           TEXT,
    _avatar_url                     TEXT,
    _extra                          JSONB
) AS $$
    DECLARE
        __extras JSONB[] := CASE _extra IS NULL WHEN true THEN ARRAY[] ELSE ARRAY[_extra] END;
    BEGIN
        INSERT INTO "Entity"."User" (uid, name, avatar_url, hides_folders, current_visible_video_count, extras)
        VALUES (_uid, _name, _avatar_url, NULL, NULL, __extras)
        ON CONFLICT DO UPDATE
        SET avatar_url =                    COALESCE(_avatar_url, avatar_url),
            extras =                        array_cat(extras, __extras)
            ;
    END $$ LANGUAGE plpgsql;

CREATE PROCEDURE "Entity"."sp_updateUserHidesFolders" (
    _uid            BIGINT,
    _hides_folders  BOOLEAN
) AS $$
    BEGIN
        UPDATE "Entity"."User"
        SET hides_folders = _hides_folders
        WHERE uid = _uid;
    END $$ LANGUAGE plpgsql;

CREATE PROCEDURE "Entity"."sp_updateUserCurrentVisibleVideoCount" (
    _uid            BIGINT,
    _count          INTEGER
) AS $$
    BEGIN
        UPDATE "Entity"."User"
        SET current_visible_video_count = _count
        WHERE uid = _uid;
    END $$ LANGUAGE plpgsql;

