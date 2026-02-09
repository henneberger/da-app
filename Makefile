.PHONY: \
	build-runtime-image \
	build-mcp-gateway-image \
	build-forum-postgres-image \
	install-operator \
	init-cluster \
	destroy-init-cluster

RUNTIME_IMAGE ?= vertx-graphql:latest
FORUM_POSTGRES_IMAGE ?= forum-postgres:pg16-wal2json

# Cluster bootstrap (used by local/dev setups).
NAMESPACE ?= default
ATLASGO_OPERATOR_NAMESPACE ?= atlas-operator
STRIMZI_OPERATOR_MANIFEST ?= https://strimzi.io/install/latest?namespace=$(NAMESPACE)
FLINK_OPERATOR_HELM_REPO ?= https://downloads.apache.org/flink/flink-kubernetes-operator-1.13.0/
FLINK_OPERATOR_HELM_REPO_NAME ?= flink-operator-repo
FLINK_OPERATOR_HELM_RELEASE ?= flink-kubernetes-operator
FLINK_OPERATOR_HELM_CHART ?= flink-kubernetes-operator

build-runtime-image:
	mvn -q -DskipTests package
	docker build -t $(RUNTIME_IMAGE) -f Dockerfile .

build-mcp-gateway-image:
	docker build -t $(MCP_GATEWAY_IMAGE) -f mcp/gateway/Dockerfile mcp/gateway

build-forum-postgres-image:
	docker build -t $(FORUM_POSTGRES_IMAGE) -f samples/postgres/Dockerfile.pgvector-wal2json samples/postgres

install-operator:
	kubectl apply -f k8s/crd
	kubectl apply -f k8s/operator/rbac.yaml
	kubectl apply -f k8s/operator/deployment.yaml

init-cluster:
	kubectl create -f '$(STRIMZI_OPERATOR_MANIFEST)' -n $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	helm status atlas-operator --namespace $(ATLASGO_OPERATOR_NAMESPACE) >/dev/null 2>&1 || \
		helm install atlas-operator oci://ghcr.io/ariga/charts/atlas-operator --create-namespace --namespace $(ATLASGO_OPERATOR_NAMESPACE)
	helm repo add $(FLINK_OPERATOR_HELM_REPO_NAME) $(FLINK_OPERATOR_HELM_REPO) --force-update
	helm repo update
	helm upgrade --install $(FLINK_OPERATOR_HELM_RELEASE) $(FLINK_OPERATOR_HELM_REPO_NAME)/$(FLINK_OPERATOR_HELM_CHART) --namespace $(NAMESPACE) --create-namespace --set webhook.create=false

destroy-init-cluster:
	- helm uninstall $(FLINK_OPERATOR_HELM_RELEASE) --namespace $(NAMESPACE)
	- helm uninstall atlas-operator --namespace $(ATLASGO_OPERATOR_NAMESPACE)
	- kubectl delete -f '$(STRIMZI_OPERATOR_MANIFEST)' -n $(NAMESPACE) --ignore-not-found
