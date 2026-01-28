FROM alpine:3.18

# 시스템 패키지 설치 (sshfs + fuse만)
RUN apk add --no-cache \
    sshfs \
    fuse \
    bash \
    openssh-client

# 마운트 포인트 생성
RUN mkdir -p /mnt/rootfs /root/.ssh

# FUSE 설정
RUN echo "user_allow_other" >> /etc/fuse.conf

# 대기 상태 유지 (kubectl exec 대기)
CMD ["tail", "-f", "/dev/null"]
