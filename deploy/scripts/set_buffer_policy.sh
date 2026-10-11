#!/bin/sh
# 렌더링된 매니페스트의 컨트롤러 env 에 N 을 넣는다.
#
#   sh set_buffer_policy.sh <rendered.yaml> <N>
#
# 컨트롤러는 N 을 시작할 때 env 에서 읽는다. 적용한 뒤 set env 로 바꾸면 rollout 이
# 두 번 일어나고, 그 사이 첫 컨트롤러는 N 없이 떠 아무것도 만들지 않는다. 그래서
# 적용하기 전에 값을 넣는다. env 가 정확히 한 번 바뀌지 않으면 실패한다.

set -eu

rendered="$1"
capacity="$2"
tmp="${rendered}.policy"

awk -v n="${capacity}" '
    pending != "" && $1 == "value:" {
        sub(/value:.*/, "value: \"" value "\"")
        count[pending]++
        pending = ""
    }
    $1 == "-" && $2 == "name:" && $3 == "BUFFER_CAPACITY" { pending = "N"; value = n }
    { print }
    END { if (count["N"] != 1) exit 3 }
' "${rendered}" > "${tmp}" || {
    rm -f "${tmp}"
    echo "[RenderFailed] controller env BUFFER_CAPACITY not found exactly once in ${rendered}"
    exit 1
}
mv "${tmp}" "${rendered}"
