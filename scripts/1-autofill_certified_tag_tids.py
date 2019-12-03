#!/usr/bin/env python3

import os
import sqlite3
import contextlib
import json

import common

(_, _, filenames) = next(os.walk("Configurations"))
filenames = filter(lambda x: x.find('certified-tags-') == 0, filenames)

for filename in filenames:
    total_tags = 0
    autofilled_tags = 0

    names_of_tags_didnt_have_tid = []
    with open('Configurations/' + filename, 'r+') as f:
        info = common.parse_certified_tags_txt(f.readlines())
        total_tags = info.total_tags
        names_of_tags_didnt_have_tid = info.names_of_tags_didnt_have_tid

        tag_name_to_tid_dict = {}
        with contextlib.closing(sqlite3.connect(common.ENTITY_DB_PATH)) as conn:
            with conn as cur:
                records = cur.execute("""
                SELECT name, tid FROM (
                    SELECT value as request_name FROM json_each(?)
                ) LEFT JOIN tag ON request_name = name
                """, (json.dumps(names_of_tags_didnt_have_tid),)).fetchall()
            tag_name_to_tid_dict = {name: tid for name, tid in records}

        final = []
        key_set = set()
        for lineno_minus_one, line in enumerate(info.lines):
            if line[0] == 't':
                tid = line[1][0]
                name = line[1][1]
                prefix = ''
                if name in key_set:
                    print("重复: 第{}行".format(lineno_minus_one + 1))
                    prefix = '/'
                else:
                    key_set.add(name)
                if tid is None:
                    tid = tag_name_to_tid_dict.get(name, None)
                    if tid == None:
                        tid = ''
                    else:
                        autofilled_tags += 1
                final.append('{}{}:{}'.format(prefix, tid, name))
            else:  # line[0] == 'e'
                final.append(line[1])

        f.seek(0)
        f.write('\n'.join(final))
        f.truncate()

    print('{}: 在 {} 行 tag 中, {} 行 tag 原先不包含 tid, 更新了其中 {} 行 tag 的 tid.'.format(
        filename, total_tags, len(names_of_tags_didnt_have_tid), autofilled_tags))
