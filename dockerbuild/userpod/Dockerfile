FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y sudo gcc vim python3 git gdb make htop python3-pip # buildkit

RUN useradd -s /bin/bash -u 1000 dcuuser && echo "dcuuser:dcuuser" | chpasswd && adduser dcuuser sudo
RUN echo 'dcuuser ALL=(ALL) NOPASSWD:ALL' | tee -a /etc/sudoers

# 동적 할당 클라이언트 스크립트 복사
COPY dalloc.py /usr/local/bin/dalloc
RUN chmod +x /usr/local/bin/dalloc

USER dcuuser
WORKDIR /home/dcuuser

RUN git config --global user.name "dcuuser" # buildkit
RUN git config --global user.email dcuuser@test.com # buildkit
RUN echo 'set nu' > /home/dcuuser/.vimrc # buildkit

# 테스트용 파이썬 파일 복사
COPY --chown=dcuuser:dcuuser test.py /home/dcuuser/test.py
RUN chmod +x /home/dcuuser/test.py