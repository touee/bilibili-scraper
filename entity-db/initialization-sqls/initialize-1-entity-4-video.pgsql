CREATE TABLE Entity.Video (
    aid             BIGINT  NOT NULL,
    title           TEXT    NOT NULL,
    uploader_uid    BIGINT  NOT NULL,

    ownership               SMALLINT    NULL,
    description             TEXT        NULL,
    publish_time            TIMESTAMP   NULL,
    create_time             TIMESTAMP   NULL,
    subregion_id            SMALLINT    NULL,
    part_count              SMALLINT    NULL,
    cover_url               TEXT        NULL,
    duration                INTEGER     NULL,
    cid_of_part_1           BIGINT      NULL,
    -- 小于 0 为削除 (各类原因), 大于 0 未知
    state                   SMALLINT    NULL,
    is_tag_list_complete    BOOLEAN     NOT NULL DEFAULT false,

    extras JSONB[] NULL,

    CONSTRAINT pkVideo_aid PRIMARY KEY (aid),
    CONSTRAINT fkVideo_uploader_uid
        FOREIGN KEY (uploader_uid)
        REFERENCES Entity."User" (uid),
    CONSTRAINT fkVideo_subregion_id
        FOREIGN KEY (subregion_id)
        REFERENCES Entity.SubregionForVideo (subregion_id)
);

CREATE INDEX idx_Video_uploader_uid 
    ON Entity.Video(uploader_uid);

CREATE PROCEDURE Entity.sp_upsertVideo(
    _aid            BIGINT,
    _title          TEXT,
    _uploader_uid   BIGINT,
    _ownership      SMALLINT,
    _description    TEXT,
    _publish_time   TIMESTAMP,
    _create_time    TIMESTAMP,
    _subregion_id   SMALLINT,
    _part_count     SMALLINT,
    _cover_url      TEXT,
    _duration       INTEGER,
    _cid_of_part_1  BIGINT,
    _state          SMALLINT,
    _extra          JSONB
) AS $$
    DECLARE
        __extras JSONB[] := CASE _extra IS NULL WHEN true THEN ARRAY[] ELSE ARRAY[_extra] END;
    BEGIN
        INSERT INTO Video (tid, name, "type", cover_url, head_cover_url, description, short_description, create_time, extras)
        VALUES (_tid, _name, "_type", _cover_url, _head_cover_url, _description, _short_description, _create_time, __extras)
        ON CONFLICT DO UPDATE
        SET ownership =                 COALESCE(_ownership, ownership),
            description =               COALESCE(_description, description),
            publish_time =              COALESCE(_publish_time, publish_time),
            create_time =               COALESCE(_create_time, create_time),
            subregion_id =              COALESCE(_subregion_id, subregion_id),
            part_count =                COALESCE(_part_count, part_count),
            cover_url =                 COALESCE(_cover_url, cover_url),
            duration =                  COALESCE(_duration, duration),
            cid_of_part_1 =             COALESCE(_cid_of_part_1, cid_of_part_1),
            state =                     COALESCE(_state, state),
            extras =                    array_cat(extras, __extras)
            ;
    END $$ LANGUAGE plpgsql;


CREATE TABLE Entity.VideoStats (
    aid BIGINT NOT NULL,

    views           INTEGER NOT NULL,
    danmakus        INTEGER NOT NULL,
    replies         INTEGER NOT NULL,
    favorites       INTEGER NOT NULL,
    coins           INTEGER NOT NULL,
    shares          INTEGER NOT NULL,
    highest_rank    INTEGER NOT NULL,
    likes           INTEGER NOT NULL,
    dislikes        INTEGER NOT NULL,

    remained    JSONB NULL,

    update_time TIMESTAMP NOT NULL,

    CONSTRAINT pkVideoStats_aid PRIMARY KEY (aid),
    CONSTRAINT fkVideoStats_aid 
        FOREIGN KEY (aid) REFERENCES Entity.Video (aid)
);

CREATE PROCEDURE Entity.sp_upsertVideoStats(
    _aid            BIGINT,
    _views          INTEGER,
    _danmakus       INTEGER,
    _replies        INTEGER,
    _favorites      INTEGER,
    _coins          INTEGER,
    _shares         INTEGER,
    _highest_rank   INTEGER,
    _likes          INTEGER,
    _dislikes       INTEGER,
    _remained       JSONB,
    _update_time    TIMESTAMP
) AS $$
    BEGIN
        INSERT INTO Entity.VideoStats (aid, views, danmakus, replies, favorites, coins, shares, highest_rank, likes, dislikes, remined, update_time)
        VALUES (_aid, _views, _danmakus, _replies, _favorites, _coins, _shares, _highest_rank, _likes, _dislikes, _remined, _update_time)
        ON CONFLICT DO UPDATE
        SET views =         _views,
            danmakus =      _danmakus,
            replies =       _replies,
            favorites =     _favorites,
            coins =         _coins,
            shares =        _shares,
            highest_rank =  _highest_rank,
            likes =         _likes,
            dislikes =      _dislikes,
            remained =      _remained,
            update_time =   _update_time
        ;
    END $$ LANGUAGE plpgsql;


CREATE TABLE Entity.VideoTag (
    aid             BIGINT NOT NULL,
    associated_tid  BIGINT NOT NULL,

    likes       INTEGER NULL,
    dislikes    INTEGER NULL,

    CONSTRAINT pkVideoTag_aid_and_associated_tid
        PRIMARY KEY (aid, associated_tid),
    CONSTRAINT fkVideoTag_aid
        FOREIGN KEY (aid) 
        REFERENCES Entity.Video (aid),
    CONSTRAINT fkVideoTag_associated_tid
        FOREIGN KEY (associated_tid) 
        REFERENCES Entity.Tag (tid)
);

CREATE INDEX idx_VideoTag_associated_tid 
    ON Entity.VideoTag (associated_tid);

CREATE PROCEDURE Entity.sp_upsertIncompleteVideoTag(
    _aid                BIGINT,
    _associated_tid     BIGINT
) AS $$
    BEGIN
        IF (SELECT is_tag_list_complete 
            FROM Video
            WHERE aid = _aid) = true THEN
            RETURN;
        END IF;
        INSERT INTO VideoTag (aid, associated_tid, likes, dislikes)
        VALUES (_aid, _associated_tid, NULL, NULL)
        ON CONFLICT DO NOTHING
        ;
    END $$ LANGUAGE plpgsql;

CREATE PROCEDURE Entity.sp_setCompleteVideoTagList(
    _aid                BIGINT,
    _associated_tids    BIGINT[],
    _likes_array        INTEGER[],
    _dislikes_array     INTEGER[]
) AS $$
    BEGIN
        ASSERT array_length(associated_tids ,1) = array_length(likes_array ,1);
        ASSERT array_length(associated_tids ,1) = array_length(dislikes_array ,1);
        DELETE FROM VideoTag
        WHERE aid = _aid;
        FOR i IN array_lower(associated_tids, 1) .. array_upper(associated_tids, 1)
        LOOP
            INSERT INTO VideoTag (aid, associated_tid, likes, dislikes)
            VALUES (_aid, _associated_tids[i], _likes_array[i], _dislikes_array[i])
            -- should not conflict
            ;
        END LOOP;
        UPDATE Video
        SET is_tag_list_complete = true
        WHERE aid = _aid
        ;
    END $$ LANGUAGE plpgsql;

CREATE FUNCTION Entity.fn_updateCertifiedVideosAfterInsertVideoTags()
    RETURNS trigger AS $$
        BEGIN
            INSERT INTO Analyzing.AutoCertifiedVideo(aid)
            SELECT DISTINCT aid
            FROM NEW
            LEFT JOIN InputCertifiedTag USING (tid)
            WHERE InputCertifiedTag.tid IS NOT NULL
                ON CONFLICT DO NOTHING
            ;
        END
    $$ LANGUAGE plpgsql;
CREATE TRIGGER tr_VideoTag_after_insert
    AFTER INSERT 
    ON Entity.VideoTag
    FOR EACH STATEMENT
    EXECUTE PROCEDURE Entity.fn_updateCertifiedVideosAfterInsertVideoTags();
    
CREATE FUNCTION Entity.fn_updateCertifiedVideosAfterDeleteVideoTags()
    RETURNS trigger AS $$
        BEGIN
            WITH VideoNotCertifiedAnyMore AS (
                SELECT aid
                FROM NEW
                LEFT JOIN VideoTag USING (aid)
                GROUP BY aid
                HAVING COALESCE(
                    SUM(EXISTS (
                            SELECT * 
                            FROM InputCertifiedTag 
                            WHERE InputCertifiedTag.tid = VideoTag.tid)
                        ), 0) = 0
            )
            DELETE FROM AutoCertifiedVideo
            USING VideoNotCertifiedAnyMore
            WHERE AutoCertifiedVideo.aid = VideoNotCertifiedAnyMore.aid
            ;
        END
    $$ LANGUAGE plpgsql;
CREATE TRIGGER tr_VideoTag_after_delete
    AFTER DELETE 
    ON Entity.VideoTag
    FOR EACH STATEMENT
    EXECUTE PROCEDURE Entity.fn_updateCertifiedVideosAfterDeleteVideoTags();
    