# path : /k8s-dynamic-allocator
# docker build -f deploy/docker/controller/Dockerfile -t harbor.cu.ac.kr/k8s_dynamic_allocator/controller:{tag} .
# docker push harbor.cu.ac.kr/k8s_dynamic_allocator/controller:{tag}

FROM python:3.11-slim

WORKDIR /app

COPY controller/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY controller .

ENV PYTHONUNBUFFERED=1

EXPOSE 9001

CMD ["python", "rest_api/manage.py", "runserver", "0.0.0.0:9001"]
