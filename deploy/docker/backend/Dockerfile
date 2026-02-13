FROM ubuntu:20.04

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    sshfs \
    fuse \
    curl \
    vim \
    iputils-ping \
    net-tools \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# fuse.conf 설정 (user_allow_other)
RUN echo "user_allow_other" >> /etc/fuse.conf

# Python 의존성 설치
COPY backend_agent/requirements.txt .
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

# Agent 코드 복사
COPY backend_agent/ .

# 마운트 포인트 생성
RUN mkdir -p /mnt/chroot /root/.ssh

# Agent 포트 노출
EXPOSE 8080
EXPOSE 8081

# Agent 서버 실행
CMD ["uvicorn", "agent:app", "--host", "0.0.0.0", "--port", "8080"]
