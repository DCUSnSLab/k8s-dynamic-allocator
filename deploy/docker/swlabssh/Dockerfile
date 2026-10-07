FROM python:3.11-alpine

ARG USER_POD_IMAGE
RUN test -n "$USER_POD_IMAGE"
ENV USER_POD_IMAGE=${USER_POD_IMAGE}

WORKDIR /dcusshk8s

COPY dcusshk8s/ /dcusshk8s/
COPY kda_config.py /dcusshk8s/kda_config.py

RUN apk add build-base
RUN apk add libffi-dev
RUN apk add openssh-keygen

RUN pip install --upgrade pip
# setup.py names the dependencies without versions, so an editable install on
# its own takes whatever is newest. kubernetes 32.0 made content_type a required
# argument of ApiClient.deserialize, which kubessh calls with two arguments in
# serialization.py, so a cache-less build fails at make_pod_spec. dcusshk8s
# already pins the working range in requirements.txt; install that first and the
# editable install finds every dependency satisfied.
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --editable .

RUN apk add vim

WORKDIR /dcusshk8s/kubessh

RUN ssh-keygen -f dummy-kubessh-host-key

WORKDIR /dcusshk8s

RUN apk add curl
RUN curl -L https://storage.googleapis.com/kubernetes-release/release/v1.16.0/bin/linux/amd64/kubectl > /usr/local/bin/kubectl
RUN chmod +x /usr/local/bin/kubectl

ENV PYTHONPATH=/dcusshk8s/
