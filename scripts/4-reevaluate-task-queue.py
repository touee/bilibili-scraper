#!/usr/bin/env python3

from typing import Set

import sqlite3
import contextlib

import common

# 视频标签(202): ~~来源视频需未通过认证?~~
# 用户收藏夹列表(302): 无限制


with contextlib.closing(sqlite3.connect(common.TASK_DB_PATH)) as conn:
    with contextlib.closing(conn.cursor()) as cur:
        cur.execute('UPDATE queue SET status = 0 WHERE status = 1')
        changes = cur.execute('SELECT changes()').fetchone()[0]
        if changes > 0:
            print('重置了 {} 个正在运行的任务的状态至 pending'.format(changes))
        cur.execute('ATTACH \'{}\' AS entity', common.ENTITY_DB_PATH)

        # 视频相关视频(201):
        # 来源视频需通过认证 (视频拥有通过认证的标签即通过认证)
        cur.execute('''
        UPDATE queue 
        SET status = CASE (
            SELECT query_id in certified_video
            ) WHEN 1 THEN 0 ELSE -2 END
        WHERE type = 201 AND status in (0, -2)
        ''')
        # 用户投稿(301):
        # 自第二页起, 收集到的用户上传的视频需至少有一件通过认证
        cur.execute('''
        WITH user_has_certified_video(uid) AS (
            SELECT uid
            FROM queue
            LEFT JOIN user ON query_id = uid
            LEFT JOIN video ON user.uid = video.uploader_uid
            WHERE type = 301 
                AND progress > 1 -- 筛掉不会去测试的用户
            GROUP BY uid
            HAVING max(aid IN certified_video) = 1
        )
        UPDATE queue
        SET status = CASE 1
            WHEN progress = 1 
            THEN 0
            WHEN query_id IN user_has_certified_video
            THEN 0
            ELSE -2
            END
        WHERE type = 301 AND status IN (0, -2)
        ''')
        # 标签信息(401)/标签默认排序(402):
        # 标签需通过认证, 或
        # 其 tag top 任务的 sample 中通过认证的视频高过一定比例

        # 收藏夹(501):
        # 自第二页起, 其包含的拥有完整标签的视频中, 通过认证的标签高过一定比例
        cur.execute('''
		WITH folder_passed_test(owner_uid, fid) AS (
			SELECT query_for_folder.owner_uid, query_for_folder.fid
			FROM queue LEFT JOIN query_for_folder ON queue.query_id = query_for_folder.folder_referrer_id
			LEFT JOIN folder_score
                    ON folder_score.owner_uid = query_for_folder.owner_uid
                        AND folder_score.fid = query_for_folder.fid
			WHERE type = 501
				AND progress > 1 -- 筛掉不会去测试的用户
				AND (certified_video_ratio >= 0.5 
					OR (certified_video_ratio_among_videos_that_have_complete_tags >= 0.8
						AND count_of_videos_that_have_complete_tags > 30))
		)
		UPDATE queue
        SET status = CASE 1
            WHEN progress = 1 THEN 0
            WHEN (SELECT owner_uid, fid FROM query_for_folder WHERE folder_referrer_id = query_id)  IN folder_passed_test
            THEN 0
            ELSE -2
            END
        WHERE type = 501 AND status IN (0, -2)
        ''')

    conn.commit()
