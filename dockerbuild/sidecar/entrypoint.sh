#!/bin/sh
echo "Starting Dynamic Allocator Sidecar..."
echo "Socket path: $SOCKET_PATH"
echo "Namespace: $NAMESPACE"

# Python 스크립트 실행
python3 sidecar.py