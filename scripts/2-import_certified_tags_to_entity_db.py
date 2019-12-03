#!/usr/bin/env python3

import os
import sqlite3
import contextlib

import common

(_, _, filenames) = next(os.walk("Configurations"))
filenames = filter(lambda x: x.find('certified-tags-') == 0, filenames)

with contextlib.closing(sqlite3.connect(common.ENTITY_DB_PATH)) as conn:
    with contextlib.closing(conn.cursor()) as cur:
        cur.execute("DROP TABLE IF EXISTS certified_tag")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS certified_tag (
                tid INTEGER PRIMARY KEY NOT NULL
            )
            """)
        for filename in filenames:
            print('{}:'.format(filename))
            info = None
            with open('Configurations/' + filename, 'r+') as f:
                info = common.parse_certified_tags_txt(f.readlines())
            tags_didnt_have_tid = len(info.names_of_tags_didnt_have_tid)
            if tags_didnt_have_tid > 0:
                print("WARNING: 有 {} 个标签尚无 tid, 将忽略.".format(tags_didnt_have_tid))
            for tid in info.get_tids():
                try:
                    cur.execute(
                        "INSERT INTO certified_tag (tid) VALUES (?)", (tid,))
                except sqlite3.IntegrityError:
                    print("重复: {}".format(tid))
    conn.commit()
