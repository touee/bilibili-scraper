# 采集策略

[TOC]

## 标签优先

### 输入

`[tag]`: (tag/detail, tag/top: 1)

`SELECT tid FROM tag WHERE tid IN unfinished_tag`: (tag/detail, tag/top: 0)

### 调度

* **tag/detail**: (来自被认证的视频/tag时: 0, 此外: freeze)
* **tag/top**:  (来自被认证的视频/tag时: 0, 此外: freeze)
* **video/tags**: (未被认证时: 1, 此外: freeze)
* video/related_videos: freeze
* user/submissions: freeze
* user/folder_list: freeze
* folder/video_items: freeze

## 稿件优先

### 输入

`SELECT uid FROM user WHERE uid IN (SELECT DISTINT uploader_uid FROM certified_video LEFT JOIN video ON certified_video.aid = video.aid)`: (user/submissions: 0, user/folder_list: freeze)

### 调度

* **tag/detail**: (来自未被认证的视频/tag时: 2, 此外: freeze)
* **tag/top**: (来自未被认证的视频/tag时: 2, 此外: freeze)
* **video/tags**: (未被认证时: 1, 此外: freeze)
* video/related_videos: freeze
* user/submissions: freeze
* user/folder_list: freeze
* folder/video_items: freeze

## 收藏优先

### 输入

`SELECT uid FROM user WHERE uid IN likely_target_user`: (user/submissions: freeze, user/folder_list: 0)

### 调度

* **tag/detail**: (来自未被认证的视频时: 3, 此外: freeze)
* **tag/top**: (来自未被认证的视频时: 3, 此外: freeze)
* **video/tags**: (未被认证时: 2, 此外: freeze)
* video/related_videos: freeze
* user/submissions: freeze
* **user/folder_list**: 0
* **folder/video_items**: 1

## 相关视频优先

### 输入

`SELECT aid FROM video WHERE aid IN related_video_of_certified_video`: (video/tags: (未被认证时: 2, 此外: freeze), video/related_videos: 0)

### 调度

* **tag/detail**: (来自未被认证的视频时: 2, 此外: freeze)
* **tag/top**: (来自未被认证的视频时: 2, 此外: freeze)
* **video/tags**: (未被认证时: 1, 此外: freeze)
* **video/related_videos**: (来自被认证的视频时: 0, 此外: freeze)
* user/submissions: freeze
* user/folder_list: freeze
* folder/video_items: freeze