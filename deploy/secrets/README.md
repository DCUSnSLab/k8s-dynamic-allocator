# SSHFS 연결 테스트
**이 가이드는 Frontend Pod와 Backend Pod 간의 SSHFS 역방향 마운트 기능을 검증하기 위한 절차입니다.**

**`/k8s-dynamic-allocator/deploy/secrets/` 경로에서 진행하세요.**

**namespaces : `swlabpods`**


-----------------
## 1. SSH 키 생성 및 리소스 배포
### SSH 키 생성
```shell
ssh-keygen -t rsa -b 4096 -C "backend-to-frontend" -f backend_ssh_key -N ""
```


### K8s 리소스 배포

k8s 리소스 파일(Secret, ConfigMap)은 보안 이슈로 스크립트로 배포합니다.

```shell
chmod +x apply-secrets.sh
./apply-secrets.sh
```

-------------
## 2. 테스트용 Pod 배포
```shell
k apply -f sshfs-test-pods.yaml
```


pod 배포 결과 : `test-frontend` pod IP 확인
```shell
NAME                          READY   STATUS    RESTARTS      AGE     IP               NODE      NOMINATED NODE   READINESS GATES
test-backend                  1/1     Running   0          6m15s   172.31.189.72    worker2   <none>           <none>
test-frontend                 1/1     Running   0          6m15s   172.31.189.93    worker2   <none>           <none>
```
--------------
## 3. 연결 테스트
### Backend 접속 및 마운트 시도
```shell
k exec - it test-backend -- bash
```

`test-backend` 내부에서 진행
```shell
# 마운트 포인트 생성
mkdir -p /mnt/frontend

# SSHFS 마운트 수행 (<$FRONTEND_IP>는 위에서 확인한 주소 사용)
sshfs -o IdentityFile=/root/.ssh/id_rsa -o StrictHostKeyChecking=no root@<$FRONTEND_IP>:/root /mnt/frontend

# 검증: Frontend에 생성된 파일 내용 확인
ls -l /mnt/frontend
cat /mnt/frontend/hello.txt
```
`hello.txt` 파일 출력 결과가 `Hello from Frontend!` 이면 성공

**실행 결과**
```shell
root@test-backend:/# mkdir -p /mnt/frontend
root@test-backend:/# sshfs -o IdentityFile=/root/.ssh/id_rsa -o StrictHostKeyChecking=no root@172.31.189.93:/root /mnt/frontend
root@test-backend:/# ls -l /mnt/frontend
total 4
-rw-r--r-- 1 root root 21 Jan  8 06:48 hello.txt
root@test-backend:/# cat /mnt/frontend/hello.txt
Hello from Frontend!
```
-----------
## 테스트 종료
### 리소스 정리
```shell
k delete pods -f sshfs-test-pods.yaml 
```
