#!/usr/bin/env python3
import os
import socket
import threading
import json
import logging
from datetime import datetime
from kubernetes import client, config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('dynamic-allocator-sidecar')


class DynamicAllocatorSidecar:
    def __init__(self):
        self.socket_path = os.environ.get('SOCKET_PATH', '/tmp/sockets/allocator.sock')
        self.namespace = os.environ.get('NAMESPACE', 'default')

        # Kubernetes 설정 로드
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except config.ConfigException:
            try:
                config.load_kube_config()
                logger.info("Loaded local Kubernetes config")
            except:
                logger.error("Failed to load Kubernetes config")

        self.k8s_client = client.CoreV1Api()

    def start_socket_server(self):
        """Unix socket 서버 시작"""
        # 기존 소켓 파일 제거
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

        # 소켓 디렉토리 생성
        os.makedirs(os.path.dirname(self.socket_path), exist_ok=True)

        # Unix socket 생성
        server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_socket.bind(self.socket_path)
        server_socket.listen(5)

        # 소켓 파일 권한 설정 (모든 사용자가 접근 가능)
        os.chmod(self.socket_path, 0o777)

        logger.info(f"Socket server started at {self.socket_path}")

        while True:
            try:
                client_socket, addr = server_socket.accept()
                # 각 클라이언트를 별도 스레드에서 처리
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket,)
                )
                client_thread.daemon = True
                client_thread.start()

            except Exception as e:
                logger.error(f"Error accepting connection: {e}")

    def handle_client(self, client_socket):
        """클라이언트 요청 처리"""
        try:
            # 데이터 수신
            data = client_socket.recv(4096).decode('utf-8')
            if not data:
                return

            logger.info(f"Received request from dev container: {data}")

            try:
                request = json.loads(data)
                command = request.get('command', '')
                args = request.get('args', [])

                logger.info(f"Processing execution request:")
                logger.info(f"   - Command: {command}")
                logger.info(f"   - Arguments: {args}")
                logger.info(f"   - Timestamp: {datetime.now().isoformat()}")

                # 응답 메시지 생성
                response = {
                    "status": "received",
                    "message": "Request received by sidecar successfully",
                    "timestamp": datetime.now().isoformat(),
                    "processed_command": command,
                    "processed_args": args
                }

                # 클라이언트에 응답 전송
                client_socket.send(json.dumps(response).encode('utf-8'))
                logger.info("Response sent back to dev container")

            except json.JSONDecodeError:
                error_response = {
                    "status": "error",
                    "message": "Invalid JSON format"
                }
                client_socket.send(json.dumps(error_response).encode('utf-8'))
                logger.error("Invalid JSON received from client")

        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            client_socket.close()


def main():
    sidecar = DynamicAllocatorSidecar()
    logger.info("Dynamic Allocator Sidecar starting...")
    logger.info(f"Socket path: {sidecar.socket_path}")
    logger.info(f"Namespace: {sidecar.namespace}")

    try:
        sidecar.start_socket_server()
    except KeyboardInterrupt:
        logger.info("Sidecar shutting down...")
    except Exception as e:
        logger.error(f"Sidecar error: {e}")


if __name__ == "__main__":
    main()