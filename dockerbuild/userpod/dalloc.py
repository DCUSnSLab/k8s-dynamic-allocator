#!/usr/bin/env python3
import socket
import json
import sys
import os
import time


def send_request_to_sidecar(command, args):
    socket_path = '/tmp/sockets/allocator.sock'

    # 소켓이 존재할 때까지 대기
    max_wait = 10
    waited = 0
    while not os.path.exists(socket_path) and waited < max_wait:
        print(f"Waiting for sidecar socket... ({waited}s)")
        time.sleep(1)
        waited += 1

    if not os.path.exists(socket_path):
        print("Error: Sidecar socket not found!")
        return False

    try:
        # Unix socket 연결
        client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client_socket.connect(socket_path)

        # 요청 데이터 생성
        request_data = {
            "command": command,
            "args": args
        }

        print(f"Sending request to sidecar: {command} {' '.join(args)}")

        # 요청 전송
        client_socket.send(json.dumps(request_data).encode('utf-8'))

        # 응답 수신
        response = client_socket.recv(4096).decode('utf-8')
        response_data = json.loads(response)

        print(f"Response from sidecar: {response_data.get('message', 'No message')}")
        print(f"Processed at: {response_data.get('timestamp', 'Unknown')}")

        client_socket.close()
        return True

    except Exception as e:
        print(f"Error communicating with sidecar: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: dalloc <command> [args...]")
        print("Example: dalloc python test.py")
        sys.exit(1)

    command = sys.argv[1]
    args = sys.argv[2:] if len(sys.argv) > 2 else []

    success = send_request_to_sidecar(command, args)
    sys.exit(0 if success else 1)