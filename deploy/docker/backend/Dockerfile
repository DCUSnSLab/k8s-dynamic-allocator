FROM python:3.11-alpine

WORKDIR /app

# 시스템 패키지 설치
RUN apk add --no-cache \
    build-base \
    libffi-dev \
    openssh-keygen \
    vim \
    curl \
    sshfs \
    bash \
    fuse

# Python 의존성 설치
COPY backend_agent/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Agent 코드 복사
COPY backend_agent/ .

# 마운트 포인트 생성
RUN mkdir -p /mnt/frontend /root/.ssh

# Agent 포트 노출
EXPOSE 8080
EXPOSE 8081

# Agent 서버 실행
CMD ["uvicorn", "agent:app", "--host", "0.0.0.0", "--port", "8080"]
