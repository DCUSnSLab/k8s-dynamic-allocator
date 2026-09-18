# SIMULATOR

이 시뮬레이터는 로컬에서 서버에 배포되어 있는 `swlabssh`에 SSH로 접속하여 여러 사용자의 `run` 요청을 반복 실행한다.

실행 전에 config의 `experiment.pool_size`(R)와 `experiment.pool_total_max`(N)를 `kda-test`의 compute Deployment에 맞추고, 대기 파드가 준비될 때까지 기다린다. 둘 중 하나라도 비어 있으면 서버를 건드리지 않는다. controller 개수, pool 모드, image 등 나머지 서버 설정은 바꾸지 않고 읽어서 `summary.json`의 `server`에 기록한다. 이를 위해 `kubectl`이 `KUBECONFIG`로 클러스터에 접근할 수 있어야 한다.

## config 파일

```text
evaluation/src/simulator/scenario_config.yaml
```

주요 필드 :

- `experiment.name` : 실험 이름. 결과 폴더 이름과 요청 ID에 사용
- `ssh.host` , `ssh.port` : `swlabssh` Node IP와 NodePort
- `users.count` : 사용자 수
- `workload.profile` : 부하 값 (`avg`, `p95_high`, `p99_peak`, `max_stress`, `nhpp_daily`, 직접 lambda 값)
- `workload.duration_minutes` : 실행 시간
- `workload.max_requests` : 최대 요청 수
- `commands.items`: 실행할 명령어와 선택 비율

## 실험 전 서버 초기화

pod 재시작만으로는 Redis(볼륨에 저장)에 이전 실행의 티켓·배정 기록이 남는다. 실험 전에 아래 명령으로 초기화한다.

```powershell
python evaluation\src\simulator\reset_server.py --clear-logs
```

- controller를 0개로 줄이고 Redis의 `kda:*` 키를 모두 삭제
- Deployment 밖에 남은 compute pod(배정 상태, cold-start pod) 삭제
- controller 복구, swlabssh·compute-general 재시작 후 대기 pod가 R/N에 맞을 때까지 대기
- `--clear-logs`: logs-pvc의 수집 로그도 삭제
- 확인 입력 없이 실행하려면 `--yes`

## 실행 방법

프로젝트 루트에서 실행

```powershell
python -m pip install -r evaluation\src\simulator\requirements.txt
```

config 파일 기준으로 실행:

```powershell
python evaluation\src\simulator\run_simulator.py
```

실행 옵션은 `scenario_config.yaml` 값을 임시로 덮어쓸 때 사용

주요 옵션 :

- `--config <path>` : 사용할 config 파일 경로 지정
- `--dry-run` : 실제 SSH 접속 없이 요청 스케줄만 출력. `max_requests`가 없으면 기본 10개만 미리보기
- `--experiment-name <name>` : 실험 이름 변경
- `--users <N>` : 사용자 수 변경
- `--profile <value>` : workload profile 변경. 예: `avg`, `p95_high`, `nhpp_daily`, `10`
- `--random-seed <seed>` : 랜덤 seed 변경
- `--duration-minutes <N>` : 실행 시간 변경
- `--max-requests <N>` : 최대 요청 수 변경

예시:

```powershell
python evaluation\src\simulator\run_simulator.py --users 1 --profile 10 --duration-minutes 1 --max-requests 1 --experiment-name smoke_swlabssh
```

위 명령은 config 파일을 수정하지 않고, 사용자 1명으로 요청 1개만 보내는 smoke test를
실행한다.

## 결과

결과는 아래 경로에 저장된다.

```text
evaluation/data/<timestamp>_<experiment.name>/simulator/
```

생성 파일:

- `config.json`: 실행에 사용한 설정 snapshot
- `requests.jsonl`: 요청별 실행 로그
- `pods.jsonl`: 실행 중 compute pod, user pod, controller pod의 생성·Ready·배정·삭제 기록 (`kubectl get pods --watch`)
- `summary.json`: 전체 요약 통계

`requests.jsonl`에는 `request_id`, `ticket_id`, `compute_pod`,
`duration_ms`, `schedule_lag_ms`, `status`, `error` 등이 저장된다.

`status` 값:

- `success` : 명령이 exit 0으로 끝남
- `exit_nonzero` : 명령이 0이 아닌 exit code로 끝남
- `timeout` : 명령 제한 시간 초과
- `error` : `swlabssh`가 명령을 받은 뒤 실패
- `ssh_error` : SSH 연결 문제로 `swlabssh`가 명령을 받지 못함(`command_delivered`가 false). 서버 쪽 실패가 아니므로 `ticket_missing`, `allocation_missing`에서 제외

## 결과 지표 계산

서버 로그를 받은 뒤 공통 지표를 계산한다.

```powershell
python evaluation\src\log_analysis\export_experiment_logs.py --run-id <run-id> --namespace kda-test --pvc logs-pvc
```
```powershell
python evaluation\src\log_analysis\experiment_metrics.py evaluation\data\<run-id> --bucket-minutes 30
```

`analysis/metrics.json`에 저장되며, `--bucket-minutes`를 주면 같은 지표를 구간별로도 계산한다. 요청은 실행된 구간이 아니라 예정된 구간에 속한다.

로그는 수집 중인 파일을 그대로 가져오므로, 파일 끝에 쓰다 만 줄이 있으면 잘라낸다.

- 요청: 상태별 개수, 서버 실패 수(`ssh_error` 제외), 시작 지연·명령 실행 시간 분포
- 컨트롤러 로그: 대기 pod가 없어 기다린 요청 비율과 대기 시간, 큐 대기 시간, 배정 시간
- `pods.jsonl`: compute·user pod의 시간 평균 점유 자원(limits 기준, 사용자 1명당 포함), compute·대기·배정 pod 수의 시간 평균과 최대

죽은 SSH 연결은 keepalive로 감지하고, 명령이 서버에 전달되기 전에 끊긴 요청은 새 연결로 한 번 다시 보낸다. 이때 `ssh_attempts`는 2가 되고, 앞 시도에서 잃은 시간은 `ssh_retry_ms`에 기록된다. `until_*_ms`는 마지막 시도부터 잰 값이다. 요청 스케줄 시작 직전에는 모든 사용자 연결을 한 번 더 확인한다.
