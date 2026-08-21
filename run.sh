#!/usr/bin/env bash
# One-command demo: kind → Helm (Redis + scheduler) → ArgoCD apps → naive vs pack utilization.
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"
BIN="${ROOT}/.bin"
mkdir -p "$BIN"
export PATH="$BIN:$PATH"

CLUSTER=vgpu-demo
NS=vgpu
SCHEDULER_PORT=8080

need_docker() {
  if docker info >/dev/null 2>&1; then
    return 0
  fi
  if sg docker -c 'docker info' >/dev/null 2>&1; then
    exec sg docker -c "\"$0\" $*"
  fi
  echo "Docker is required (and your user must reach the docker socket)." >&2
  exit 1
}

install_tool() {
  local name="$1" url="$2"
  if command -v "$name" >/dev/null 2>&1; then
    return 0
  fi
  if [[ -x "$BIN/$name" ]]; then
    return 0
  fi
  echo "==> installing $name"
  local tmp
  tmp="$(mktemp -d)"
  curl -fsSL "$url" -o "$tmp/asset"
  case "$name" in
    kubectl)
      mv "$tmp/asset" "$BIN/kubectl" && chmod +x "$BIN/kubectl"
      ;;
    kind)
      mv "$tmp/asset" "$BIN/kind" && chmod +x "$BIN/kind"
      ;;
    helm)
      tar -xzf "$tmp/asset" -C "$tmp"
      mv "$tmp/linux-amd64/helm" "$BIN/helm" && chmod +x "$BIN/helm"
      ;;
  esac
  rm -rf "$tmp"
}

need_docker "$@"

ARCH=amd64
KVER=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
install_tool kubectl "https://dl.k8s.io/release/${KVER}/bin/linux/${ARCH}/kubectl"
install_tool kind "https://kind.sigs.k8s.io/dl/v0.27.0/kind-linux-${ARCH}"
install_tool helm "https://get.helm.sh/helm-v3.17.2-linux-${ARCH}.tar.gz"

echo "==> build vgpu-scheduler"
CGO_ENABLED=0 go build -o vgpu-scheduler .

echo "==> kind cluster ($CLUSTER)"
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --wait 120s
else
  echo "    (reusing existing cluster)"
  kubectl cluster-info --context "kind-${CLUSTER}" >/dev/null
fi
kubectl config use-context "kind-${CLUSTER}" >/dev/null

echo "==> container image → kind"
sg docker -c "docker build -t vgpu-scheduler:local ." 2>/dev/null || docker build -t vgpu-scheduler:local .
kind load docker-image vgpu-scheduler:local --name "$CLUSTER"
# Redis image is already on the host from other demos; load it so kind need not pull.
if docker image inspect redis:7-alpine >/dev/null 2>&1; then
  kind load docker-image redis:7-alpine --name "$CLUSTER"
fi

echo "==> namespace $NS"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Helm: redis + vgpu-scheduler"
helm upgrade --install vgpu-redis ./charts/redis -n "$NS" --wait --timeout 120s
helm upgrade --install vgpu-scheduler ./charts/vgpu-scheduler -n "$NS" \
  --set image.repository=vgpu-scheduler \
  --set image.tag=local \
  --set image.pullPolicy=Never \
  --wait --timeout 120s

echo "==> ArgoCD (GitOps control plane)"
if ! kubectl get ns argocd >/dev/null 2>&1; then
  kubectl create namespace argocd
  kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.13.3/manifests/install.yaml
fi
echo "    waiting for argocd-server…"
kubectl -n argocd rollout status deployment/argocd-server --timeout=180s
kubectl apply -f argocd/application.yaml
kubectl -n argocd get applications

echo "==> port-forward scheduler :${SCHEDULER_PORT}"
kubectl -n "$NS" port-forward svc/vgpu-scheduler "${SCHEDULER_PORT}:8080" >/tmp/vgpu-pf.log 2>&1 &
PF_PID=$!
cleanup() {
  kill "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:${SCHEDULER_PORT}/healthz" >/dev/null; then
    break
  fi
  sleep 0.5
done
if ! curl -sf "http://127.0.0.1:${SCHEDULER_PORT}/healthz" >/dev/null; then
  echo "scheduler not reachable on :${SCHEDULER_PORT}" >&2
  cat /tmp/vgpu-pf.log >&2 || true
  exit 1
fi

echo "==> concurrent workloads (naive → pack)"
python3 submit_jobs.py --url "http://127.0.0.1:${SCHEDULER_PORT}"

echo
echo "Demo complete. Cluster: kind-${CLUSTER}  namespace: ${NS}"
echo "Inspect: kubectl -n ${NS} get pods && kubectl -n argocd get applications"
