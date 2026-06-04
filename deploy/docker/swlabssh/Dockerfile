FROM python:3.11-alpine

WORKDIR /dcusshk8s

COPY dcusshk8s/ /dcusshk8s/
COPY kda_config.py /dcusshk8s/kda_config.py

RUN apk add build-base
RUN apk add libffi-dev
RUN apk add openssh-keygen

RUN pip install --upgrade pip
RUN pip install --editable .

RUN apk add vim

WORKDIR /dcusshk8s/kubessh

RUN ssh-keygen -f dummy-kubessh-host-key

WORKDIR /dcusshk8s

RUN apk add curl
RUN curl -L https://storage.googleapis.com/kubernetes-release/release/v1.16.0/bin/linux/amd64/kubectl > /usr/local/bin/kubectl
RUN chmod +x /usr/local/bin/kubectl

ENV PYTHONPATH=/dcusshk8s/
