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

# 컨트롤러와 같은 의존성 집합으로 돌린다. 테스트가 지금 직접 쓰는 외부 모듈은
# yaml 하나뿐이고 그것은 kubernetes 가 끌고 오지만, 프로덕션과 같은 환경에서
# 돌리는 편이 회귀를 잡는 장치로서 맞다. requirements.txt 가 바뀔 때만 다시 깔린다.
COPY controller/requirements.txt /src/controller/requirements.txt
RUN pip install --no-cache-dir -r /src/controller/requirements.txt

# 레이아웃을 저장소 그대로 둬야 한다. 테스트는 __file__ 기준으로
# parents[1] 에서 controller/services, controller/manifests 를,
# parents[2] 에서 deploy/scripts/deploy.sh 를 읽는다.
COPY controller /src/controller
COPY deploy/scripts /src/deploy/scripts

RUN python -m unittest discover -s controller/tests -t controller/tests -v
