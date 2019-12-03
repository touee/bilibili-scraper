from typing import List, Tuple, Optional

ENTITY_DB_PATH = 'workdir/entities.sqlite3'
TASK_DB_PATH = 'workdir/tasks.sqlite3'


class BadFormatException(Exception):
    def __init__(self, lineno: int, exception: Exception):
        self.lineno = lineno
        self.exception = exception


class CertifiedTagsInfo:
    def __init__(self, lines: List[Tuple[Optional[int], Optional[str]]], total_tags: int, names_of_tags_didnt_have_tid: List[str]):
        self.lines = lines
        self.total_tags = total_tags
        self.names_of_tags_didnt_have_tid = names_of_tags_didnt_have_tid

    def get_tids(self):
        out = []
        for line in self.lines:
            if line[0] == 't':
                if line[1][0] is not None:
                    out.append(line[1][0])
        return out


def parse_certified_tags_txt(input_lines: List[str]) -> CertifiedTagsInfo:
    output_linse = []
    total_tags = 0
    names_of_tags_didnt_have_tid = []
    for lineno_minus_one, line in enumerate(input_lines):
        line = line.strip()
        if len(line) == 0 or line[0] == '#':
            output_linse.append(('e', line))
        else:
            try:
                total_tags += 1
                parts = line.split(':')
                tid = parts[0].strip()
                name = parts[1].strip()
                if tid == '':
                    tid = None
                    if name == '':
                        print('?')
                        exit(2)
                    else:
                        names_of_tags_didnt_have_tid.append(name)
                else:
                    tid = int(tid)
                output_linse.append(('t', (tid, name)))
            except IndexError as e:
                raise BadFormatException(lineno_minus_one + 1, e)
    return CertifiedTagsInfo(output_linse, total_tags, names_of_tags_didnt_have_tid)
