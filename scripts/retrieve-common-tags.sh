#!/usr/bin/env sh

set -e

export NAMES='["东方", "東方", "东方PROJECT", "東方Project", "東方project"]'

echo 'fething characters' >&2
export CHARACTERS=$(curl -s 'https://thwiki.cc/官方角色顺序' |
    pup '#mw-content-text ul ul > li json{}' |
    # jq '[.[].text|[., [.|split("·")|.[0]]]]|flatten|unique'
    jq '[.[].text]'
)

echo 'fething titles' >&2
export TITLES=$(curl -s 'https://thwiki.cc/简称与英文称呼' |
    pup '#mw-content-text .wikitable json{}'|
    jq '[.[:-1][] | until(.tag=="tr"; .children[]?) | .children[:2][] | select(.tag=="td") | .text | split(" ")[] | split("、")[] | select((.|length)>2)]'|
    sed -e 's/ //g'
)

echo 'combining' >&2
echo | jq -n -c --argjson names "$NAMES" --argjson characters "$CHARACTERS" --argjson titles "$TITLES" \
'$names + $characters + $titles | unique'
