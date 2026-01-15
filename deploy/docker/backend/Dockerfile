FROM python:3.6.8-alpine

WORKDIR /root

RUN apk add build-base
# RUN apk add pip
RUN apk add libffi-dev
RUN apk add openssh-keygen
RUN apk add vim
RUN apk add curl

RUN apk add sshfs bash

RUN pip install --upgrade pip

RUN curl -L https://storage.googleapis.com/kubernetes-release/release/v1.16.0/bin/linux/amd64/kubectl > /usr/local/bin/kubectl
RUN chmod +x /usr/local/bin/kubectl

