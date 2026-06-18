# SIMULATOR

이 시뮬레이터는 로컬에서 서버에 배포되어 있는 `swlabssh`에 SSH로 접속하여 여러 사용자의 `run` 요청을 반복 실행한다.

시뮬레이터는 Kubernetes 리소스를 직접 수정하지 않는다. Pool 크기, controller 개수, namespace, image 등은 서버에서 먼저 배포해두고 실행한다.

## config 파일

```text
simulator/scenario_config.yaml
```

주요 필드 :

- `experiment.name` : 실험 이름. 결과 폴더 이름과 요청 ID에 사용
- `ssh.host` , `ssh.port` : `swlabssh` Node IP와 NodePort
- `users.count` : 사용자 수
- `workload.profile` : 부하 값 (`avg`, `p95_high`, `p99_peak`, `max_stress`, `nhpp_daily`, 직접 lambda 값)
- `workload.duration_minutes` : 실행 시간
- `workload.max_requests` : 최대 요청 수
- `commands.items`: 실행할 명령어와 선택 비율

## 실행 방법

프로젝트 루트에서 실행

```powershell
python -m pip install -r simulator\requirements.txt
```

config 파일 기준으로 실행:

```powershell
python simulator\run_simulator.py
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
python simulator\run_simulator.py --users 1 --profile 10 --duration-minutes 1 --max-requests 1 --experiment-name smoke_swlabssh
```

위 명령은 config 파일을 수정하지 않고, 사용자 1명으로 요청 1개만 보내는 smoke test를
실행한다.

## 결과

결과는 아래 경로에 저장된다.

```text
simulator/results/<timestamp>_<experiment.name>/
```

생성 파일:

- `config.json`: 실행에 사용한 설정 snapshot
- `requests.jsonl`: 요청별 실행 로그
- `summary.json`: 전체 요약 통계

`requests.jsonl`에는 `request_id`, `ticket_id`, `compute_pod`,
`duration_ms`, `schedule_lag_ms`, `status`, `error` 등이 저장된다.
