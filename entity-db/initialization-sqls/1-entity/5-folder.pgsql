CREATE TABLE "Entity"."Folder" (
    -- 或许可以用 media_id((fid*100 + owner_uid%100)?) 替代?
    folder_reference_id BIGINT
        GENERATED ALWAYS AS IDENTITY (
        INCREMENT BY 1
        START WITH 1),
    owner_uid           INTEGER     NOT NULL,
    fid                 INTEGER     NOT NULL,
    name                TEXT        NOT NULL,
    capacity            INTEGER     NOT NULL,
    current_item_count  INTEGER     NOT NULL,
    create_time         TIMESTAMP   NOT NULL,
    modify_time         TIMESTAMP   NOT NULL,

    extras JSONB[] NULL,

    CONSTRAINT "pkFolder_folder_reference_id"
        PRIMARY KEY (folder_reference_id),
    CONSTRAINT "fkFolder_owner_uid"
        FOREIGN KEY (owner_uid)
        REFERENCES "Entity"."User" (uid),
    CONSTRAINT "uqFolder_owner_uid_and_fid" UNIQUE (owner_uid, fid)
);

CREATE PROCEDURE "Entity"."sp_upsertFolder"(
    _owner_uid              INTEGER,
    _fid                    INTEGER,
    _name                   TEXT,
    _capacity               INTEGER,
    _current_item_count     INTEGER,
    _create_time            TIMESTAMP,
    _modify_time            TIMESTAMP,
    _extra                  JSONB
) AS $$
    DECLARE
        __extras JSONB[] := CASE _extra IS NULL WHEN true THEN ARRAY[] ELSE ARRAY[_extra] END;
    BEGIN
        INSERT INTO "Video" (tid, name, "type", cover_url, head_cover_url, description, short_description, create_time, extras)
        VALUES (_tid, _name, "_type", _cover_url, _head_cover_url, _description, _short_description, _create_time, __extras)
        ON CONFLICT DO UPDATE
        SET capacity =              COALESCE(_capacity, capacity),
            current_item_count =    COALESCE(_current_item_count, current_item_count),
            create_time =           COALESCE(_create_time, create_time),
            modify_time =           COALESCE(_modify_time, modify_time),
            extras =                array_cat(extras, __extras)
            ;
    END $$ LANGUAGE plpgsql;


CREATE TABLE "Entity"."FolderVideoItem" (
    folder_reference_id BIGINT      NOT NULL,
    item_aid            BIGINT      NOT NULL,
    favorite_time       TIMESTAMP   NOT NULL,

    CONSTRAINT "pkFolderVideoItem_folder_reference_id_and_item_aid"
        PRIMARY KEY (folder_reference_id, item_aid),
    CONSTRAINT "fkFolderVideoItem_folder_reference_id"
        FOREIGN KEY (folder_reference_id)
        REFERENCES "Entity"."Folder" (folder_reference_id),
    CONSTRAINT "fkFolderVideoItem_item_aid"
        FOREIGN KEY (item_aid)
        REFERENCES "Entity"."Video" (aid)
);

CREATE INDEX "idx_FolderVideoItem_item_aid"
    ON "Entity"."FolderVideoItem" (item_aid);

CREATE PROCEDURE "Entity"."sp_upsertFolderVideoItems" (
    _owner_uid      BIGINT,
    _fid            BIGINT,
    _item_aids      BIGINT[],
    _favorite_times BIGINT[]
) AS $$
    DECLARE
        _folder_reference_id BIGINT;
    BEGIN
        SELECT folder_reference_id
        INTO _folder_reference_id
        FROM "Folder"
        WHERE owner_uid = _owner_uid AND fid = _fid
        ;
        FOR i IN array_lower(_item_aids, 1) .. array_upper(_item_aids, 1)
        LOOP
            INSERT INTO FolderVideoItem (folder_reference_id, item_aid, favorite_time)
            VALUES (_folder_reference_id, _item_aids[i], _favorite_times[i])
            ON CONFLICT DO UPDATE
            SET favorite_time = _favorite_time
            ;
        END LOOP;
    END $$ LANGUAGE plpgsql;

