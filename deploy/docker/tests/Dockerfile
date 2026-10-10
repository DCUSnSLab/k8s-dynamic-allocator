# path : /k8s-dynamic-allocator
# docker build -f deploy/docker/tests/Dockerfile .
#
# 컨트롤러 단위 테스트를 이미지 빌드로 돌린다. 테스트가 실패하면 RUN 이 실패하고
# 그래서 빌드가 멈춘다.
#
# 왜 빌드로 돌리나: 이 Jenkins 에이전트는 호스트의 도커 데몬을 쓰고(docker.sock 을
# hostPath 로 마운트) 워크스페이스는 에이전트 파드의 emptyDir 이다. 그래서
# docker.image().inside() 가 거는 `-v <워크스페이스>:<워크스페이스>` 는 호스트에
# 없는 경로를 빈 디렉터리로 마운트하고, durable-task 가 스크립트를 찾지 못해
# "process apparently never started" 로 죽는다(빌드 48 실측). docker build 는
# 컨텍스트만 보내므로 이 토폴로지에서 동작한다 - 다른 이미지 단계가 그 증거다.

FROM python:3.11-slim

WORKDIR /src

# 테스트가 쓰는 외부 모듈은 yaml 하나뿐이다(나머지는 stdlib). 그것만 깐다.
#
# 처음에는 "프로덕션과 같은 환경" 을 위해 controller/requirements.txt 전체를
# 깔았는데, 그게 빌드 51 을 깨뜨렸다. PyPI 에서 약 20MB 를 내려받다 읽기
# 타임아웃이 났다. 더 나쁜 것은 CI 에이전트 파드가 빌드마다 다른 노드에 떠서
# 도커 레이어 캐시가 남지 않는다는 점이다(빌드 50 과 51 이 서로 다른 노드).
# 즉 매 빌드가 전량 재다운로드를 감당해야 했다. 테스트 게이트가 네트워크
# 때문에 깨지면 게이트로서 쓸모가 없으므로 받는 양을 25 배 줄였다.
#
# 앞으로 테스트가 다른 외부 모듈을 쓰게 되면 여기에 명시적으로 추가한다.
# requirements.txt 전체를 다시 끌어오지 말 것.
RUN pip install --no-cache-dir --retries 10 --timeout 60 pyyaml

# 레이아웃을 저장소 그대로 둬야 한다. 테스트는 __file__ 기준으로
# parents[1] 에서 controller/services, controller/manifests 를,
# parents[2] 에서 deploy/scripts/deploy.sh 를 읽는다.
COPY controller /src/controller
COPY deploy/scripts /src/deploy/scripts

RUN python -m unittest discover -s controller/tests -t controller/tests -v
