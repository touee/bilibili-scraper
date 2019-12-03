#!/usr/bin/env python3

import sqlite3
import contextlib

import common

with contextlib.closing(sqlite3.connect(common.ENTITY_DB_PATH)) as conn:
    with contextlib.closing(conn.cursor()) as cur:
        cur.execute('''
        CREATE VIEW IF NOT EXISTS certified_video AS
            SELECT DISTINCT video.aid
            FROM certified_tag 
            LEFT JOIN video_tag ON certified_tag.tid = video_tag.tid
            LEFT JOIN video ON video_tag.aid = video.aid
        ''')
        cur.execute('''
        CREATE VIEW IF NOT EXISTS tag_score AS
        SELECT 
            tid, 
            count(video.aid) AS counted_video_count,
            avg(certified_video.aid IS NOT NULL) AS score
        FROM video
        INNER JOIN video_tag ON video.aid = video_tag.aid
        LEFT JOIN certified_video ON video.aid = certified_video.aid
        WHERE video.is_tag_list_complete = 1
        GROUP BY tid
        ''')
        cur.execute('''
        CREATE VIEW IF NOT EXISTS folder_stat AS
        WITH folder_video_item_stat AS
        (
            WITH folder_video_item_tag AS
            (
                WITH folder_video_item AS
                (
                    SELECT
                        owner_uid, fid,
                        json_extract(video_item.value, '$[0]') AS video_item_aid,
                        is_tag_list_complete AS with_complete_tags
                    FROM 
                        folder, json_each(video_items) AS video_item
                    LEFT JOIN video ON video_item_aid = video.aid
                )
                SELECT
                    owner_uid, fid,
                    video_item_aid,
                    (video_tag.tid IN certified_tag) AS is_certified_tag,
                    with_complete_tags
                FROM folder_video_item
                LEFT JOIN video_tag ON video_item_aid = video_tag.aid
            )
            SELECT
                owner_uid, fid,
                video_item_aid,
                ifnull(max(is_certified_tag), 0) AS has_certified_tag,
                with_complete_tags
            FROM folder_video_item_tag
            GROUP BY owner_uid, fid, video_item_aid
        )
        SELECT
            owner_uid, fid,
            count(video_item_aid) AS count_of_collected_videos,
            sum(has_certified_tag) AS count_of_certified_videos,
            sum(with_complete_tags) AS count_of_videos_that_have_complete_tags,
            sum(has_certified_tag AND with_complete_tags) AS count_of_certified_videos_that_have_complete_tags
        FROM folder_video_item_stat
        GROUP BY owner_uid, fid
        ''')
        cur.execute('''
        CREATE VIEW IF NOT EXISTS folder_score AS
        SELECT 
            owner_uid, fid,
            count_of_certified_videos*1.0/count_of_collected_videos 
                AS certified_video_ratio,
            count_of_collected_videos,
            ifnull(count_of_certified_videos_that_have_complete_tags*1.0/count_of_videos_that_have_complete_tags, 0)
                AS certified_video_ratio_among_videos_that_have_complete_tags,
            count_of_videos_that_have_complete_tags
        FROM folder_stat
        ''')
    conn.commit()
