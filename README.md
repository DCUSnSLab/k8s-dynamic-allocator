# k8s-dynamic-allocator

작업 공간과 연산 자원의 생명주기를 분리한 쿠버네티스 기반 대화형 실습 환경

- 사용자는 SSH로 접속해 **User Pod**(개인 작업 공간, PVC 영속)에 접속
- 명령을 실행할 때만 **Compute Pod**(고사양 연산 자원)를 할당
- Compute Pod는 미리 띄워 둔 대기 파드 → 할당 지연 최소화
- 작업 종료 시 Compute Pod 삭제, User Pod와 데이터는 유지

## 구성 요소

- **controller** : 할당·큐·풀 용량 제어. Django REST + Redis. 리더 선출로 다중 인스턴스 운영
- **compute_agent** : Compute Pod 내부 에이전트. SSHFS로 User Pod 파일시스템 마운트 및 명령 수행
- [**dcusshk8s**](https://github.com/DCUSnSLab/dcusshk8s) *(submodule)* : SSH 게이트웨이 및 User Pod 생성/관리

## 프로젝트 구조

```
/
├── controller/
│   ├── manifests/            Compute Pod 정의. controller가 읽어서 런타임에 생성
│   ├── rest_api/             실행 요청을 받는 REST API 엔드포인트
│   └── services/
│       ├── compute/          Compute Pod 할당·해제와 pool 크기 조절
│       ├── infra/            쿠버네티스 API 접근, 리더 선출, 리소스 변경 감지
│       └── queue/            요청 대기열과 처리 상태 관리 (Redis)
├── compute_agent/            Compute Pod에서 실행. 작업 공간을 마운트 및 명령 수행
├── dcusshk8s/                SSH 게이트웨이 (submodule)
│   ├── kubessh/              사용자 인증과 User Pod 생성·접속
│   └── dockerbuild/          User Pod 이미지 정의
├── deploy/
│   ├── base/                 모든 환경이 공유하는 쿠버네티스 리소스
│   ├── overlays/<환경>/       환경마다 달라지는 값 정의 (namespace, StorageClass 등)
│   ├── docker/               compute, controller, swlabssh 이미지 정의
│   ├── scripts/              배포 절차와 검증·진단
│   └── secrets/              SSH 키. 수동 적용하며 CI/CD에서 제외
├── evaluation/
│   ├── src/simulator/        다수 사용자의 요청을 재현하는 부하 생성기
│   ├── src/log_analysis/     실험 로그 수집과 지표 산출
│   └── data/                 실험 결과
├── Jenkinsfile               CI/CD 파이프라인
└── kda_config.py             이미지 공통으로 공유하는 런타임 설정
```

## 이미지

tag : `BUILD_NUMBER-GIT_SHA7` 불변 태그

| 이미지 | 빌드 소스 | 배포 형태 |
|---|---|---|
| `controller` | `controller/` | Deployment |
| `compute_pod` | `compute_agent/` | `controller`가 런타임 생성 |
| `swlabssh` | `dcusshk8s/` | Deployment |
| `user_pod` | `dcusshk8s/dockerbuild/` | `swlabssh`가 런타임 생성 |


## 배포

 Kustomize + Jenkins 파이프라인 구조

| 항목 | dev | 운영(미정) |
|---|---|---|
| namespace | `kda-test` | `swlabpods` |
| Redis PVC | `normal-r3` (Longhorn) | `normal-r3` (Longhorn) |
| 로그 PVC | `normal-r3` (RWX, 100Gi) | - |
| SSH 접속 | NodePort 30622 | - |




- 매니페스트 : `deploy/base` + `deploy/overlays/<환경>`. 렌더 확인 `kubectl kustomize deploy/overlays/dev`
- 파이프라인 : `Jenkinsfile`. 이미지 4종 빌드 → 런타임 계약 검증 → Harbor push → `bootstrap → logging → redis → controller → compute → swlabssh` 순차 적용
- 실패 시 롤백 없이 중단하고 진단 출력. PVC, SSH 키, 기존 Pod은 삭제하지 않음
- `deploy/base`에 리소스 추가 시 `k8s-dynamic-allocator/deploy-stage` 라벨 필수
