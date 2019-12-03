
## 获取标签分数
```sql
SELECT tag_score.tid, name, score, counted_video_count 
FROM tag_score LEFT JOIN tag ON tag_score.tid = tag.tid 
WHERE counted_video_count >= 10 AND tag_score.tid NOT IN certified_tag
ORDER BY score DESC, counted_video_count DESC
```

## 插入遗漏的相关视频任务
```sql
INSERT INTO queue
SELECT NULL AS task_id, 201 AS type, query_id, 0 AS priority, 0 AS status, 0 AS attempts, NULL AS progress, NULL AS metadata, '[[-1, 0]]' AS referrers
FROM queue WHERE type = 202
```

## 从 referrers 提取出 tag 所需的 sample, 然后设为 tag 任务的 metadata
```sql
WITH sample_table(tid, sample) AS (
	SELECT tid, json_group_array(aid) AS sample
	FROM (
		WITH tag_task_id(tid) AS (
			SELECT task_id 
			FROM queue 
			WHERE type = 40X
		)
		SELECT tag_task.query_id AS tid, video_task.query_id AS aid, json_extract(value, '$[0]') AS referrer_task_id
		FROM queue AS video_task, json_each(video_task.referrers)
		LEFT JOIN queue AS tag_task ON tag_task.task_id = referrer_task_id
		WHERE video_task.type IN (201, 202) AND referrer_task_id in tag_task_id
		GROUP BY tid, aid
	)
	GROUP BY tid
)
UPDATE queue 
SET metadata ='{ "sample_videos": ' || (SELECT sample FROM sample_table WHERE query_id = tid) || ' }'
WHERE type = 40X AND query_id IN (SELECT tid FROM sample_table)
```

## 去除错误的 referer 信息
```sql
WITH tag_top_task_id(tid) AS (
	SELECT task_id 
	FROM queue 
	WHERE type = 402
)
UPDATE queue
SET referrers = (
	WITH referrer(ref_id, ts) AS (
		SELECT 
			json_extract(value, '$[0]') AS ref_id,
			json_extract(value, '$[1]') AS ts
		FROM json_each(referrers)
		WHERE ref_id NOT IN tag_top_task_id
	)
	SELECT json_group_array(json_array(ref_id, ts))
	FROM referrer
)
```

## 补上遗漏的用户/标签
```sql
-- 用户
--INSERT INTO queue 
SELECT NULL AS task_id, new_task_id, user.uid AS query_id, 0 AS priority, -2 AS status, 0 AS attempts, 1 AS progress, NULL AS metadata, '[[-1, -1]]' AS referrers
--SELECT *
FROM user 
LEFT JOIN (SELECT * FROM queue WHERE type = 301) AS x ON user.uid = x.query_id 
CROSS JOIN (SELECT column1 AS new_task_id FROM (VALUES (301), (302)))
WHERE task_id IS NULL
```

## 手动调整视频相关任务的优先级
```sql
-- 相关视频
WITH x AS (
	SELECT aid FROM queue
	LEFT JOIN video ON queue.query_id = video.aid
	WHERE queue.type = 201 AND status = 0 AND title REGEXP '([东東]方|车万)'
)
UPDATE queue SET priority = 1 WHERE type = 201 AND query_id IN x

-- 标签
SELECT title, aid FROM queue
LEFT JOIN video ON queue.query_id = video.aid
WHERE queue.type = 202 AND status = 0 AND priority != -1 AND title REGEXP '([东東]方|车万)'

SELECT title, aid FROM queue
LEFT JOIN video ON queue.query_id = video.aid
WHERE queue.type = 202 AND priority = -1 AND title REGEXP '([东東]方|车万)' AND tags IS NULL

WITH x AS (
	SELECT aid FROM queue
	LEFT JOIN video ON queue.query_id = video.aid
	WHERE queue.type = 202 AND status = 0 AND title REGEXP '([东東]方|车万)' AND tags IS NULL
)
UPDATE queue SET priority = 1 WHERE type = 202 AND query_id IN x
```

## 手动调整用户稿件任务的优先级
```sql
WITH user_that_has_uploaded_certified_videos AS (
	SELECT uid, count(certified_video.aid) AS count_of_certified_videos
	FROM certified_video 
	LEFT JOIN video ON certified_video.aid = video.aid
	LEFT JOIN user ON video.uploader_uid = user.uid
	LEFT JOIN queue ON user.uid = queue.query_id
	WHERE queue.type = 301 AND status != -1
	GROUP BY user.uid
)
UPDATE queue
SET 
	status = 0, 
	priority = 1 + ((
		SELECT count_of_certified_videos 
		FROM user_that_has_uploaded_certified_videos 
		WHERE uid = query_id)*1.0 / (SELECT count(aid) FROM video WHERE uploader_uid = query_id))
WHERE 
	type = 301
	AND query_id IN (SELECT uid FROM user_that_has_uploaded_certified_videos)
```

##
```sql
WITH user_certified_videos AS (
	SELECT name, uid, count(certified_video.aid) AS count_of_certified_videos, json_group_array(certified_video.aid) AS cefrtified_videos
	FROM certified_video 
	LEFT JOIN video ON certified_video.aid = video.aid
	LEFT JOIN user ON video.uploader_uid = user.uid
	GROUP BY user.uid
)
SELECT 
	name, uid, 
	count(aid) AS count_of_videos, 
	count_of_certified_videos, 
	count_of_certified_videos*1.0/count(aid) AS ratio, 
	cefrtified_videos
FROM user_certified_videos
LEFT JOIN video ON user_certified_videos.uid = video.uploader_uid
GROUP BY uid
ORDER BY ratio, count_of_certified_videos DESC
```

## 新旧 views
由于 video-tag 脱离 video, 独立成表而改变
<table>
<tr>
<th>name</th><th>old</th><th>new</th>
</tr>
<td>certified_video</td>
<td>
```sql
	SELECT DISTINCT aid
	FROM certified_tag LEFT JOIN (
		SELECT aid, json_extract(video_tag.value, '$[0]') AS video_tid
		FROM video, json_each(tags) AS video_tag
 	) ON video_tid = certified_tag.tid
```
</td>
<td>
```sql
	SELECT DISTINCT video.aid
	FROM certified_tag 
	LEFT JOIN video_tag ON certified_tag.tid = video_tag.tid
	LEFT JOIN video ON video_tag.aid = video.aid
```
</td>
</tr>
<tr>
<td>
tag_score
</td>
<td>
```sql
 	SELECT
		x.tid,
		count(aid) AS counted_video_count,
		avg(ifnull(aid in certified_video, 0)) AS score
	FROM (
		SELECT aid, json_extract(video_tag.value, '$[0]') AS tid
		FROM video, json_each(tags) AS video_tag
		WHERE json_array_length(json_extract(video.tags, '$[0]')) = 3
	) AS x
	WHERE x.tid NOT IN certified_tag
	GROUP BY x.tid
```
</td>
<td>
```sql
	SELECT 
		tid, 
		count(video.aid) AS counted_video_count,
		avg(certified_video.aid IS NOT NULL) AS score
	FROM video
	INNER JOIN video_tag ON video.aid = video_tag.aid
	LEFT JOIN certified_video ON video.aid = certified_video.aid
	WHERE video.is_tag_list_complete = 1
	GROUP BY tid
```
</td>
</tr>
<tr>
<td>
folder_stat
</td>
<td>
```sql
	WITH folder_video_item_stat AS
	(
		WITH folder_video_item_tag AS
		(
			WITH folder_video_item AS
			(
				SELECT
				owner_uid, fid,
				json_extract(video_item.value, '$[0]') AS video_item_aid,
				tags,
				ifnull(json_array_length(tags) = 0 OR json_array_length(json_extract(tags, '$[0]')) = 3, 0) AS with_complete_tags
				FROM 
					folder, json_each(video_items) AS video_item
				LEFT JOIN video ON video_item_aid = video.aid
				)
			SELECT
				owner_uid, fid,
				video_item_aid,
				(json_extract(video_item_tag.value, '$[0]') IN certified_tag) AS is_certified_tag,
				with_complete_tags
			FROM folder_video_item
			LEFT JOIN json_each(CASE tags IS NULL OR tags = '[]' WHEN 1 THEN '[null]' ELSE tags END)
			AS video_item_tag
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
```
</td>
<td>
```sql
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
		sum(has_certified_tag AND with_complete_tags) 
  		AS count_of_certified_videos_that_have_complete_tags
	FROM folder_video_item_stat
	GROUP BY owner_uid, fid
```
</td>
</tr>
<tr>
<td>
folder_score
</td>
<td>
```sql
	SELECT 
		owner_uid, fid,
		count_of_certified_videos*1.0/count_of_collected_videos 
			AS certified_video_ratio,
		count_of_collected_videos,
		ifnull(count_of_certified_videos_that_have_complete_tags*1.0/count_of_videos_that_have_complete_tags, 0)
			AS certified_video_ratio_among_videos_that_have_complete_tags,
		count_of_videos_that_have_complete_tags
	FROM folder_stat
```
</td>
<td>
	SELECT 
		owner_uid, fid,
		count_of_certified_videos*1.0/count_of_collected_videos 
			AS certified_video_ratio,
		count_of_collected_videos,
		ifnull(count_of_certified_videos_that_have_complete_tags*1.0/count_of_videos_that_have_complete_tags, 0)
			AS certified_video_ratio_among_videos_that_have_complete_tags,
		count_of_videos_that_have_complete_tags
	FROM folder_stat
</td>
</tr>
</table>

## 修正标签是否完整的数据
```sql
WITH x AS (
	SELECT video_tag.aid
	FROM video_tag
	LEFT JOIN video ON video_tag.aid = video.aid
	WHERE is_tag_list_complete = 0
	GROUP BY video_tag.aid
	HAVING sum(likes IS NOT NULL) > 0
)
UPDATE video SET is_tag_list_complete = 1 WHERE video.aid IN x
```

## 手动激活收藏夹任务
```sql
WITH missed_folders AS (
	SELECT folder_reference_id FROM folder_score
	LEFT JOIN query_for_folder ON folder_score.owner_uid = query_for_folder.owner_uid AND folder_score.fid = query_for_folder.fid
	LEFT JOIN queue ON query_for_folder.folder_reference_id = queue.query_id
	WHERE 
		queue.type = 501 AND queue.status != -1
		AND (certified_video_ratio >= 0.5 
			OR  certified_video_ratio_among_videos_that_have_complete_tags >= 1.0/3*2)
)
UPDATE queue
SET status = 0, priority = 1
WHERE type = 501 AND query_id IN missed_folders
```