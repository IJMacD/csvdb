#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/../../vars.sh

sudo useradd -u $CONTAINER_USER_ID ${APPNAME} || echo "User ${APPNAME} already exists."

# Ensure we're using local Ubuntu docker
docker context use default

# create the k3d registry if not present
if [ -z "$(k3d registry list | grep ${LOCAL_REGISTRY_NAME})" ];
then
  k3d registry create registry.localhost --port 0.0.0.0:$LOCAL_REGISTRY_PORT
fi

# make sure no other cluster is running
k3d cluster stop --all

# delete the entire cluster first (if it exists) so we can start from scratch
k3d cluster delete ${APPNAME}

k3d cluster create ${APPNAME} --config ${SCRIPT_DIR}/k3d-config.yml \
  --volume ${SCRIPT_DIR}/../../:/ijmacd/csvdb@all

# for i in "mariadb:11.1"; do
#   docker pull docker.io/bitnami/${i}
#   docker tag docker.io/bitnami/${i} ${LOCAL_REGISTRY}/bitnami/${i}
#   docker push ${LOCAL_REGISTRY}/bitnami/${i}
# done

# Bootstrap the helm dependencies
for project in "${PROJECTS}"; do
  helm dependency build $SCRIPT_DIR/../chart/${project}/
done

mkdir -p ~/.kube
k3d kubeconfig merge ${APPNAME} --output ${KUBECONFIG}

# Apply local secrets

${SCRIPT_DIR}/create-tls-secret.sh

# for f in "${SCRIPT_DIR}/secrets/*"; do
#   [ -e "$f" ] && \
#     echo "no secrets to apply" || \
#     kubectl apply -f $f --namespace ${APPNAME}
# done