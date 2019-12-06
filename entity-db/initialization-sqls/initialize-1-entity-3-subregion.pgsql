CREATE TABLE Entity.SubregionForVideo (
    -- (视频之外的媒介也被算成了分区, 
    -- 其中存在大于 SMALLINT 最大范围的数字. 
    -- 但这里只存视频的分区)
    subregion_id            SMALLINT    NULL,
    name                    TEXT        NULL,

    CONSTRAINT pkSubregionForVideo_subregion_id
        PRIMARY KEY (subregion_id)
);

CREATE PROCEDURE Entity.sp_upsertSubregion(
    _subregion_id    SMALLINT,
    _name            TEXT
) AS $$
    BEGIN
        INSERT INTO Entity.SubregionForVideo (subregion_id, name)
        VALUES (_subregion_id, _name)
        ON CONFLICT DO NOTHING;
    END $$ LANGUAGE plpgsql;

-- CREATE FUNCTION Entity.fn_getSubregionReferenceID(
--     _subregion_id    SMALLINT,
--     _name            TEXT
-- ) RETURNS SMALLINT AS $$
--     DECLARE
--         __name TEXT := CASE _name = '' WHEN true THEN NULL ELSE _name END;
--         _subregion_reference_id SMALLINT;
--     BEGIN
--         IF _subregion_id IS NULL AND __name IS NULL THEN
--             -- 信息不足
--             RETURN NULL;
--         END IF;
--         IF _subregion_id IS NOT NULL THEN
--             -- 理想情况: subregion_id 已知
--             SELECT subregion_reference_id 
--             INTO _subregion_reference_id
--             FROM Entity.SubregionForVideo 
--             WHERE subregion_id = _subregion_id
--             ;
--             IF _subregion_reference_id IS NOT NULL THEN
--                 -- 返回已生成的 reference_id
--                 RETURN _subregion_reference_id;
--             END IF;
--             -- 返回新生成的 reference_id
--             INSERT INTO Entity.SubregionForVideo (subregion_id, name)
--             VALUES (_subregion_id, __name)
--             RETURNING subregion_reference_id AS _subregion_reference_id
--             ;
--             RETURN _subregion_reference_id;
--         END IF;
--         -- 放弃治疗, 因为即使非空的分区名也可能重复 ("资讯")
--         RETURN NULL;
--     END
--     $$ LANGUAGE plpgsql;