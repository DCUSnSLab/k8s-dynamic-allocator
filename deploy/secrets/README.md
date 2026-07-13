# SSHFS 연결 테스트
**이 가이드는 User Pod와 Compute Pod 간의 SSHFS 역방향 마운트 기능을 검증하기 위한 절차입니다.**

**경로 : `/k8s-dynamic-allocator/deploy/secrets/`**

**namespaces : `swlabpods`**


-----------------
## 1. SSH 키 생성 및 리소스 배포
### SSH 키 생성
```shell
ssh-keygen -t rsa -b 4096 -C "compute-to-user" -f compute_ssh_key -N ""
```
<br/>

### K8s 리소스 배포

k8s 리소스 파일(Secret, ConfigMap)은 보안 이슈로 스크립트로 배포합니다.

```shell
chmod +x apply-secrets.sh
./apply-secrets.sh
```
<br/>


## 2. 테스트용 Pod 배포
```shell
k apply -f sshfs-test-pods.yaml
```

<br/>

`test-user` pod에 SSHFS 설정이 완료되면 `Running` 상태가 됩니다.

pod 배포 결과 : `test-user` pod IP 확인
```shell
NAME                          READY   STATUS    RESTARTS      AGE     IP               NODE      NOMINATED NODE   READINESS GATES
test-compute                  1/1     Running   0          6m15s   172.31.189.72    worker2   <none>           <none>
test-user                 1/1     Running   0          6m15s   172.31.189.93    worker2   <none>           <none>
```

<br/>

## 3. 연결 테스트
### Compute 접속 및 마운트 시도
```shell
k exec -it test-compute -- bash
```

`test-compute` 내부에서 진행
```shell
# 마운트 포인트 생성
mkdir -p /mnt/user

# SSHFS 마운트 수행 (<$USER_POD_IP>는 `test-user` pod IP 사용)
sshfs -o IdentityFile=/root/.ssh/id_rsa -o StrictHostKeyChecking=no root@<$USER_POD_IP>:/root /mnt/user

# 검증: User에 생성된 파일 내용 확인
ls -l /mnt/user
cat /mnt/user/hello.txt
```
<br/>

`hello.txt` 파일 출력 결과가 `Hello from User!` 이면 성공

**실행 결과**
```shell
root@test-compute:/# mkdir -p /mnt/user
root@test-compute:/# sshfs -o IdentityFile=/root/.ssh/id_rsa -o StrictHostKeyChecking=no root@172.31.189.93:/root /mnt/user
root@test-compute:/# ls -l /mnt/user
total 4
-rw-r--r-- 1 root root 21 Jan  8 06:48 hello.txt
root@test-compute:/# cat /mnt/user/hello.txt
Hello from User!
```
<br/>

## 테스트 종료
### 리소스 정리
```shell
k delete -f sshfs-test-pods.yaml
```
