# path : /k8s-dynamic-allocator
# docker build -f deploy/docker/controller/Dockerfile -t harbor.cu.ac.kr/k8s_dynamic_allocator/controller:{tag} .
# docker push harbor.cu.ac.kr/k8s_dynamic_allocator/controller:{tag}

FROM python:3.11-slim

ARG COMPUTE_POD_IMAGE
RUN test -n "$COMPUTE_POD_IMAGE"
ENV COMPUTE_POD_IMAGE=${COMPUTE_POD_IMAGE}

WORKDIR /app

COPY controller/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY controller .
COPY kda_config.py .

ENV PYTHONUNBUFFERED=1

# WSGI 경로에는 --config-file 을 넘길 자리가 없으므로 환경변수로 준다.
ENV KDA_CONFIG_FILE=/app/kda_config.py

# 동시 요청을 받는 자리라 스레드 수와 accept 대기열을 넉넉히 둔다. 할당 핸들러는
# Redis·쿠버네티스 호출로 수백 ms~수 초 블록되므로, 대기열이 짧으면 피크에서 들어온
# 연결을 커널이 RST 로 끊는다 - 실측으로 요청의 9~11% 가 이렇게 실패했다.
ENV GUNICORN_THREADS=64
ENV GUNICORN_BACKLOG=2048
ENV GUNICORN_TIMEOUT=120

EXPOSE 9001

# 워커는 1개다. 리컨실러·리더 선출 같은 배경 스레드가 워커 프로세스 안에서 도므로,
# 워커를 늘리면 한 파드 안에 그것들이 여러 벌 생긴다. 동시성은 스레드로 받는다
# (핸들러가 I/O 로 블록되는 종류라 스레드가 맞는 축이다). preload 도 쓰지 않는다 -
# fork 전에 배경 스레드가 떠 버린다.
CMD ["sh", "-c", "exec gunicorn config.wsgi:application --chdir /app/rest_api --bind 0.0.0.0:9001 --workers 1 --worker-class gthread --threads ${GUNICORN_THREADS} --backlog ${GUNICORN_BACKLOG} --timeout ${GUNICORN_TIMEOUT} --graceful-timeout 30 --keep-alive 5"]
