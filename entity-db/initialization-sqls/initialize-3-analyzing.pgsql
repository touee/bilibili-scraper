CREATE SCHEMA Analyzing;
CREATE TABLE Analyzing.InputCertifiedTag (
    tid BIGINT NOT NULL,

    CONSTRAINT pkInputCertifiedTag_tid
        PRIMARY KEY (tid)
    -- 不建立 fk, 因为 tid 可能尚未存在于 Entity 中
);
CREATE PROCEDURE Analyzing.sp_setCertifiedTags(
    _tags   BIGINT[]
) AS $$
    DECLARE
        _tid BIGINT;
    BEGIN
        TRUNCATE TABLE InputCertifiedTag;
        FOREACH _tid IN ARRAY _tags LOOP
            INSERT INTO InputCertifiedTag (tid)
            VALUES (_tid)
            ;
        END LOOP;
        TRUNCATE AutoCertifiedVideo;
        INSERT INTO AutoCertifiedVideo
        SELECT aid 
        FROM vw_CertifiedVideo
        ;
    END $$ LANGUAGE plpgsql;
CREATE TABLE Analyzing.AutoCertifiedVideo (
    aid BIGINT NOT NULL,

    CONSTRAINT pkAutoCertifiedVideo_aid
        PRIMARY KEY (aid),
    CONSTRAINT fkAutoCertifiedVideo_aid
        FOREIGN KEY (aid)
        REFERENCES Entity.Video (aid)
);
CREATE VIEW Analyzing.vw_CertifiedVideo AS
    SELECT DISTINCT aid
    FROM Analyzing.InputCertifiedTag
    LEFT JOIN Entity.VideoTag ON associated_tid = tid;
CREATE FUNCTION Analyzing.fn_updateCertifiedVideoAfterModifyCertifiedTags()
    RETURNS trigger AS $$
        BEGIN
            TRUNCATE TABLE AutoCertifiedVideo;
            INSERT INTO AutoCertifiedVideo
            SELECT aid
            FROM Analyzing.InputCertifiedTag;
        END
    $$ LANGUAGE plpgsql;

-- CREATE TRIGGER tr_InputCertifiedTag_after_modify
--     AFTER INSERT OR DELETE OR UPDATE 
--     ON Analyzing.InputCertifiedTag
--     FOR EACH STATEMENT
--     DEFERRABLE INITIALLY DEFERRED
--     EXECUTE PROCEDURE Analyzing.fn_updateCertifiedVideoAfterModifyCertifiedTags();
