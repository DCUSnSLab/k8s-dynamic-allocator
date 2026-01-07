#!/bin/bash

set -e

NAMESPACE="swlabpods"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRIVATE_KEY="${SCRIPT_DIR}/backend_ssh_key"
PUBLIC_KEY="${SCRIPT_DIR}/backend_ssh_key.pub"

echo "================================================"
echo "    Kubernetes Secret/ConfigMap Deployment"
echo "================================================"
echo ""

# Check if keys exist
if [ ! -f "$PRIVATE_KEY" ]; then
    echo -e "------ Private key not found ------\nPrivate key : \n$PRIVATE_KEY"
    echo ""
    echo "Please generate SSH keys first:"
    echo "  cd $(dirname ${BASH_SOURCE[0]})"
    echo "  ssh-keygen -t rsa -b 4096 -C 'backend-to-frontend' -f backend_ssh_key -N ''"
    echo ""
    exit 1
fi

if [ ! -f "$PUBLIC_KEY" ]; then
    echo -e "------ Public key not found ------\nPublic key : \n$PUBLIC_KEY"
    exit 1
fi

echo "------ SSH keys found ------"
echo "  Private: $PRIVATE_KEY"
echo "  Public:  $PUBLIC_KEY"
echo ""

# Check namespace
if ! kubectl get namespace "$NAMESPACE" &>/dev/null; then
    echo "Creating namespace: $NAMESPACE"
    kubectl create namespace "$NAMESPACE"
    echo ""
fi

# Create Secret from file
echo "Creating Secret: backend-ssh-key"
kubectl create secret generic backend-ssh-key \
    --from-file=id_rsa="$PRIVATE_KEY" \
    --from-file=id_rsa.pub="$PUBLIC_KEY" \
    --namespace="$NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -

echo "------ Secret created/updated ------"
echo ""

# Create ConfigMap from file
echo "Creating ConfigMap: backend-public-key"
kubectl create configmap backend-public-key \
    --from-file=id_rsa.pub="$PUBLIC_KEY" \
    --namespace="$NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -

echo "------ ConfigMap created/updated ------"
echo ""

# Verify
echo "Verifying..."
echo ""
echo "Secret:"
kubectl get secret backend-ssh-key -n "$NAMESPACE" -o jsonpath='{.metadata.name}{" (created at "}{.metadata.creationTimestamp}{")"}{"\n"}'
echo ""
echo "ConfigMap:"
kubectl get configmap backend-public-key -n "$NAMESPACE" -o jsonpath='{.metadata.name}{" (created at "}{.metadata.creationTimestamp}{")"}{"\n"}'
echo ""

echo "================================================"
echo "      Deployment completed successfully"
echo "================================================"
