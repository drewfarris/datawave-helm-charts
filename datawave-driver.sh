#!/bin/bash
USE_EXISTING_ZOOKEEPER=${USE_EXISTING_ZOOKEEPER:-false}
USE_EXISTING_HADOOP=${USE_EXISTING_HADOOP:-false}
INIT_LOCAL_HADOOP=${INIT_LOCAL_HADOOP:-false}
USING_MINIKUBE=false
BASEDIR="$( cd -- "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 || exit; pwd -P )"
DATAWAVE_STACK="${BASEDIR}"/datawave-stack
HELM_CHART=oci://ghcr.io/nationalsecurityagency/datawave-helm-charts/charts/datawave-system
HELM_CHART_VERSION=1.0.0
NAMESPACE=default
MYSQL_OPERATOR_CHART_VERSION=${MYSQL_OPERATOR_CHART_VERSION:-2.3.0}
# Optional second values file, applied after the prompted base values file.
DATAWAVE_VALUES_OVERLAY=${DATAWAVE_VALUES_OVERLAY:-}
# Floci is an independent release; enabling it never migrates Accumulo files.
DATAWAVE_ENABLE_FLOCI=${DATAWAVE_ENABLE_FLOCI:-true}
FLOCI_NAMESPACE=${FLOCI_NAMESPACE:-aws-emulation}
FLOCI_RELEASE=${FLOCI_RELEASE:-floci}
FLOCI_CLIENT_NAMESPACE=${FLOCI_CLIENT_NAMESPACE:-default}
FLOCI_VALUES_FILE=${FLOCI_VALUES_FILE:-}
FLOCI_EVIDENCE_DIR=${FLOCI_EVIDENCE_DIR:-}
if [ "$DATAWAVE_ENABLE_FLOCI" = true ]; then
  DATAWAVE_ACCUMULO_STORAGE=${DATAWAVE_ACCUMULO_STORAGE:-floci}
else
  DATAWAVE_ACCUMULO_STORAGE=${DATAWAVE_ACCUMULO_STORAGE:-hdfs}
fi
FLOCI_ACCUMULO_VOLUME=${FLOCI_ACCUMULO_VOLUME:-s3a://dwv-accumulo/accumulo}

guard_fresh_accumulo() {
  case "$DATAWAVE_ACCUMULO_STORAGE" in
    hdfs)
      local releases current_values
      releases=$(helm list -n "$NAMESPACE" --all -o json) || return 1
      if jq -e 'any(.[]; .name == "dwv")' <<<"$releases" >/dev/null; then
        current_values=$(helm get values dwv -n "$NAMESPACE" -o json) || return 1
        if jq -e '."dwv-accumulo".hybridStorage.enabled == true' <<<"$current_values" >/dev/null; then
          echo "ERROR: Cannot switch a hybrid instance to HDFS-only; no migration path is provided." >&2
          return 1
        fi
      fi
      return 0 ;;
    floci)
      if [ "$DATAWAVE_ENABLE_FLOCI" != true ]; then
        echo "ERROR: Floci storage requires DATAWAVE_ENABLE_FLOCI=true." >&2
        return 1
      fi
      python3 "$BASEDIR/floci/fresh/guard.py" --namespace "$NAMESPACE" ;;
    *) echo "ERROR: DATAWAVE_ACCUMULO_STORAGE must be floci or hdfs." >&2; return 1 ;;
  esac
}

setup_floci() {
  if [ "$DATAWAVE_ENABLE_FLOCI" = false ]; then
    echo "Floci deployment disabled. Existing Floci resources are retained."
    return 0
  fi
  if [ "$DATAWAVE_ENABLE_FLOCI" != true ]; then
    echo "ERROR: DATAWAVE_ENABLE_FLOCI must be true or false." >&2
    return 1
  fi
  local args=(--namespace "$FLOCI_NAMESPACE" --release "$FLOCI_RELEASE"
              --client-namespace "$FLOCI_CLIENT_NAMESPACE")
  [ -z "$FLOCI_VALUES_FILE" ] || args+=(--values "$FLOCI_VALUES_FILE")
  [ -z "$FLOCI_EVIDENCE_DIR" ] || args+=(--evidence "$FLOCI_EVIDENCE_DIR")
  if [ "$DATAWAVE_ACCUMULO_STORAGE" = floci ]; then
    local bucket=${FLOCI_ACCUMULO_VOLUME#s3a://}
    args+=(--bucket "${bucket%%/*}")
  fi
  python3 "$BASEDIR/floci/tools/deploy.py" "${args[@]}" "$@"
}

function ready_helm_charts() {
  while true; do
    local default_chart_mode=remote
    [ "$DATAWAVE_ACCUMULO_STORAGE" != floci ] || default_chart_mode=local
    read -p "Enter chart mode (local,remote) [$default_chart_mode]: " chart_mode
    chart_mode=${chart_mode:-$default_chart_mode}
    if [ "$DATAWAVE_ACCUMULO_STORAGE" = floci ] && [ "$chart_mode" != local ]; then
      echo "ERROR: Fresh hybrid storage requires the local charts in this checkout." >&2
      return 1
    fi
    # Check if input is one of the accepted values
    if [[ -z "$chart_mode" || "$chart_mode" == "" || "$chart_mode" == "local" || "$chart_mode" == "remote" ]]; then
        echo "Using Chart Mode: ${chart_mode:-remote}"
        break
    else
        echo "Invalid input. Please use 'local', 'remote', or leave blank to default."
    fi
  done


  if [ "$chart_mode" == "remote" ]; then
    HELM_CHART=oci://ghcr.io/nationalsecurityagency/datawave-helm-charts/charts/datawave-system
    HELM_CHART_VERSION=1.0.2
  else
    echo "Chart mode is local. Proceed to package all dependencies for the umbrella chart and assemble them"
    if ! command -v yq &> /dev/null
    then
        echo "ERROR: yq is not installed or not found in your PATH."
        echo "To install yq, you can follow these steps:"
        echo ""
        echo "For Linux/macOS:"
        echo "  sudo wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/local/bin/yq"
        echo "  sudo chmod +x /usr/local/bin/yq"
        echo ""
        echo "For macOS with Homebrew:"
        echo "  brew install yq"
        echo ""
        echo "For Windows:"
        echo "  Download the latest release from https://github.com/mikefarah/yq/releases and add it to your PATH."
        echo ""
        exit 1
    fi
    # Package a clean copy so stale dependency archives cannot enter the release.
    LOCAL_CHART_WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/datawave-charts.XXXXXX") || exit 1
    trap 'rm -rf -- "${LOCAL_CHART_WORKDIR}"' EXIT
    tar -C "$BASEDIR" --exclude='.git' --exclude='*.tgz' \
      --exclude='ghcr-image-pull-secret.yaml' -cf "$LOCAL_CHART_WORKDIR/source.tar" . || exit 1
    tar -C "$LOCAL_CHART_WORKDIR" -xf "$LOCAL_CHART_WORKDIR/source.tar" || exit 1
    package_helm_dependencies "$LOCAL_CHART_WORKDIR/datawave-stack" || exit 1
    helm package "$LOCAL_CHART_WORKDIR/datawave-stack" --destination "$LOCAL_CHART_WORKDIR" || exit 1
    local chart_name chart_version
    chart_name=$(yq eval -r '.name' "$DATAWAVE_STACK/Chart.yaml") || exit 1
    chart_version=$(yq eval -r '.version' "$DATAWAVE_STACK/Chart.yaml") || exit 1
    HELM_CHART="$LOCAL_CHART_WORKDIR/$chart_name-$chart_version.tgz"
  fi
}

package_helm_dependencies() {
    local base_dir="$1"
    local chart_file="$base_dir"/Chart.yaml

    local dependencies dep dep_path
    dependencies=$(yq eval -r '.dependencies // [] | .[].name' "$chart_file") || return 1
    for dep in $dependencies; do
        dep_path=$(yq eval -r ".dependencies[] | select(.name == \"$dep\") | .path // \"\"" "$chart_file") || return 1
        if [ -n "$dep_path" ]; then
            echo "Packaging dependency: $dep"
            package_helm_dependencies "$base_dir/$dep_path" || return 1
            
            # Package the dependency and copy it to the charts directory
            mkdir -p "$base_dir/charts" || return 1
            helm package "$base_dir/$dep_path" --destination "$base_dir/charts" || return 1
        else
            echo "ERROR: Dependency $dep in $chart_file has no local path specified."
            return 1
        fi
    done
}

function update_core_dns() {
  if ${USE_EXISTING_ZOOKEEPER} && ${USE_EXISTING_HADOOP}; then
    "${BASEDIR}"/updateCorefile.sh coredns.corefile-both.template
  elif ${USE_EXISTING_ZOOKEEPER}; then
    "${BASEDIR}"/updateCorefile.sh coredns.corefile-zookeeper.template
  elif ${USE_EXISTING_HADOOP}; then
    "${BASEDIR}"/updateCorefile.sh coredns.corefile-hadoop.template
  else
    "${BASEDIR}"/updateCorefile.sh coredns.corefile-default.template
  fi
}

# Function to check if a Kubernetes cluster is running
function check_k8s_cluster() {
  echo "Checking if Kubernetes is running..."
  kubectl cluster-info &> /dev/null
  if [ $? -ne 0 ]; then
    echo "No Kubernetes cluster found. Deploying Minikube..."
    start_minikube
  else
    echo "Active Kubeconfig found. Would you like to use this cluster (no means minikube should be deployed)? [yes] :"
    read user_input

    if [ "$user_input" = "no" ]; then
      echo "Starting Minikube cluster..."
      start_minikube
    else
      echo "Minikube cluster not started. Using active Kubeconfig"
fi
  fi
}

function set_namespace() {
  echo "Set Namespace [default] :"
  read namespace
  NAMESPACE=${namespace:-default}

  # Check if the namespace exists
  kubectl get namespace $NAMESPACE &> /dev/null

  if [ $? -ne 0 ]; then
    echo "Namespace '$NAMESPACE' does not exist. Creating namespace."
    kubectl create namespace $NAMESPACE
  else
    echo "Namespace '$NAMESPACE' exists."
  fi
}

# Function to start Minikube
function start_minikube() {

  USING_MINIKUBE=true

  echo "Do you want run Minikube purge first? (yes/no) [no]: "
  read user_input
  
  if [ "$user_input" = "yes" ]; then
      echo "Purging the minikube cluster"
      minikube delete --all --purge
  else
      echo "Minikube cluster not purged."
  fi
  local minikube_args=(start --nodes 3 --cpus 4 --memory 8192 --disk-size 20480)
  [ "$DATAWAVE_ACCUMULO_STORAGE" != floci ] || minikube_args+=(--driver=docker)
  minikube "${minikube_args[@]}"
  if [ $? -eq 0 ]; then
    echo "Minikube started successfully."
  else
    echo "Failed to start Minikube. Exiting."
    exit 1
  fi

  echo "Configure minikube for Datawave Helm charts"
  minikube addons enable ingress
  minikube kubectl -- delete -A ValidatingWebhookConfiguration ingress-nginx-admission
  minikube kubectl -- patch deployment -n ingress-nginx ingress-nginx-controller --type='json' -p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value":"--enable-ssl-passthrough"}]'


  # update default storage provisioner, set csi-hostpath-sc as the default storage driver
  echo "Enabling volumesnapshots and csi-hostpath-driver"
  minikube addons enable volumesnapshots
  minikube addons enable csi-hostpath-driver
  minikube addons disable storage-provisioner
  minikube addons disable default-storageclass
  minikube kubectl -- patch storageclass csi-hostpath-sc -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'

  echo "Minikube configured successfully"
}

# Function to perform Helm operations
function preload_docker_image() {
  local image="$1"

  echo "Checking for local image $image"
  if docker image inspect "$image" > /dev/null 2>&1; then
    echo "$image exists locally, skipping pull.."
  else
    echo "$image does not exist locally, pulling.."
    docker pull "$image"
    if [ $? -eq 0 ]; then
      echo "$image pulled successfully"
    else
      echo "Failed to pull required image $image. Exiting."
      exit 1
    fi
  fi

  echo "Loading $image into minikube"
  minikube image load "$image"
  if [ $? -eq 0 ]; then
    echo "$image loaded into minikube successfully."
  else
    echo "Failed to load $image into minikube. Exiting."
    exit 1
  fi
  
}

function configure_repository_credentials(){
  if [ ! -e "${BASEDIR}"/ghcr-image-pull-secret.yaml ]; then
    echo "Github Username: "
    read username

    echo "Github Access Token: "
    read -s pat
  
    "${BASEDIR}"/create-image-pull-secrets.sh $username $pat
 
  fi
  kubectl -n $NAMESPACE apply -f "${BASEDIR}"/ghcr-image-pull-secret.yaml
}

function create_secrets(){
  SECRET_NAME=certificates-secret
  MYSQL_SECRET_NAME=mysql-secret

  kubectl  -n $NAMESPACE get secret $SECRET_NAME &> /dev/null
  if [ $? -ne 0 ]; then
    echo "Secret '$SECRET_NAME' does not exist. Creating secret."
    kubectl -n $NAMESPACE create secret generic $SECRET_NAME --from-file=keystore.p12="${DATAWAVE_STACK}"/certificates/keystore.p12 --from-file=truststore.jks="${DATAWAVE_STACK}"/certificates/truststore.jks
  else
    echo "Secret '$SECRET_NAME' already exists."
  fi

  kubectl  -n $NAMESPACE get secret $MYSQL_SECRET_NAME &> /dev/null
  if [ $? -ne 0 ]; then
    echo "Secret '$MYSQL_SECRET_NAME' does not exist. Creating secret."
    kubectl -n $NAMESPACE create secret generic $MYSQL_SECRET_NAME --from-literal=rootPassword=changeme --from-literal=rootUser=datawave --from-literal=rootHost=%
  else
    echo "Secret '$MYSQL_SECRET_NAME' already exists."
  fi
}



function ghcr_login() {
  echo "Logging in to GHCR for docker and helm"
  if test -f "${BASEDIR}"/ghcr-image-pull-secret.yaml; then
    # File path
    FILE_PATH="${BASEDIR}/ghcr-image-pull-secret.yaml"

    # Extract the base64-encoded .dockerconfigjson value
    ENCODED_JSON=$(grep '.dockerconfigjson:' $FILE_PATH | awk '{print $2}')

    # Decode the JSON value
    DECODED_JSON=$(echo $ENCODED_JSON | base64 --decode)

    # Extract the base64-encoded auth value for ghcr.io
    ENCODED_AUTH=$(echo $DECODED_JSON | jq -r '.auths["ghcr.io"].auth')

    # Decode the auth value to get username:password
    AUTH=$(echo $ENCODED_AUTH | base64 --decode)

    # Split the username and password
    USERNAME=$(echo $AUTH | cut -d ':' -f 1)
    PASSWORD=$(echo $AUTH | cut -d ':' -f 2)
    echo $PASSWORD | docker login ghcr.io --username $USERNAME --password-stdin 
    if [ $? -eq 0 ]; then
      echo "Docker login successful."
    else
      echo "Failed to login to docker. Check credendials. Exiting."
      exit 1
    fi
    echo $PASSWORD | helm registry login ghcr.io --username $USERNAME --password-stdin
    if [ $? -eq 0 ]; then
      echo "Helm login successful."
    else
      echo "Failed to login to helm. Check credendials. Exiting."
      exit 1
    fi
  fi
}

function update_hosts_file_for_hadoop() {
  echo "$(minikube ip) namenode.datawave.org" | sudo tee -a /etc/hosts
  echo "$(minikube ip) resourcemanager.datawave.org" | sudo tee -a /etc/hosts
  echo "$(minikube ip) historyserver.datawave.org" | sudo tee -a /etc/hosts
}

function configure_etc_hosts(){
  sudo sed -i "/^$/d" /etc/hosts
  sudo sed -i "/.*datawave\.org.*/d" /etc/hosts
  sudo sed -i "/.*zookeeper.*/d" /etc/hosts
  sudo sed -i "/.*hdfs.*/d" /etc/hosts
  sudo sed -i "/.*yarn.*/d" /etc/hosts
  echo "$(minikube ip) accumulo.datawave.org" | sudo tee -a /etc/hosts
  echo "$(minikube ip) web.datawave.org" | sudo tee -a /etc/hosts
  echo "$(minikube ip) dictionary.datawave.org" | sudo tee -a /etc/hosts
  
  if ${USE_EXISTING_ZOOKEEPER}; then
    EXTRA_HELM_ARGS="${EXTRA_HELM_ARGS} --set charts.zookeeper.enabled=false"
    echo "$(minikube ip | cut -f1,2,3 -d .).1 zookeeper" | sudo tee -a /etc/hosts
  fi

  if ${USE_EXISTING_HADOOP}; then
    EXTRA_HELM_ARGS="${EXTRA_HELM_ARGS} --set charts.hadoop.enabled=false"
    echo "$(minikube ip | cut -f1,2,3 -d .).1 hdfs-nn" | sudo tee -a /etc/hosts
    echo "$(minikube ip | cut -f1,2,3 -d .).1 hdfs-dn" | sudo tee -a /etc/hosts
    echo "$(minikube ip | cut -f1,2,3 -d .).1 yarn-rn" | sudo tee -a /etc/hosts
    echo "$(minikube ip | cut -f1,2,3 -d .).1 yarn-nm" | sudo tee -a /etc/hosts
    echo "$(minikube ip | cut -f1,2,3 -d .).1 namenode.datawave.org" | sudo tee -a /etc/hosts
    echo "$(minikube ip | cut -f1,2,3 -d .).1 resourcemanager.datawave.org" | sudo tee -a /etc/hosts
    echo "$(minikube ip | cut -f1,2,3 -d .).1 historyserver.datawave.org" | sudo tee -a /etc/hosts
  else
    update_hosts_file_for_hadoop
  fi

}

function setup_mysql_operator() {
  # The operator chart rejects the default namespace. Keep its release separate
  # from DataWave so deploying or upgrading dwv cannot replace the operator.
  helm repo add mysql-operator https://mysql.github.io/mysql-operator/ || return 1
  helm repo update mysql-operator || return 1
  helm upgrade --install dwv-mysql-operator mysql-operator/mysql-operator \
    --version "$MYSQL_OPERATOR_CHART_VERSION" \
    --namespace mysql-operator --create-namespace --wait --timeout 5m0s || return 1
}

function helm_install() {
  echo "Enter path values file to use [${DATAWAVE_STACK}/values.yaml]: "
  read values_file

  echo "Starting Helm Deployment"

  local values_args=(-f "${values_file:-$DATAWAVE_STACK/values.yaml}")
  if [ -n "$DATAWAVE_VALUES_OVERLAY" ]; then
    if [ ! -f "$DATAWAVE_VALUES_OVERLAY" ]; then
      echo "ERROR: Values overlay does not exist: $DATAWAVE_VALUES_OVERLAY"
      exit 1
    fi
    values_args+=(-f "$DATAWAVE_VALUES_OVERLAY")
  fi

  if [ "$DATAWAVE_ACCUMULO_STORAGE" = floci ]; then
    local hybrid_values="$LOCAL_CHART_WORKDIR/fresh-hybrid-values.yaml"
    local prepare_args=(--namespace "$NAMESPACE" --values "${values_file:-$DATAWAVE_STACK/values.yaml}"
                        --output "$hybrid_values" --floci-namespace "$FLOCI_NAMESPACE"
                        --floci-release "$FLOCI_RELEASE" --s3-volume "$FLOCI_ACCUMULO_VOLUME")
    [ -z "$DATAWAVE_VALUES_OVERLAY" ] || prepare_args+=(--overlay "$DATAWAVE_VALUES_OVERLAY")
    python3 "$BASEDIR/floci/fresh/prepare.py" "${prepare_args[@]}" || return 1
    values_args+=(-f "$hybrid_values")
  fi
  # shellcheck disable=SC2086
  helm -n "$NAMESPACE" upgrade --install dwv "$HELM_CHART" "${values_args[@]}" ${EXTRA_HELM_ARGS} --wait --timeout 15m0s
  if [ $? -eq 0 ]; then
    echo "Helm install successful."
  else
    echo "Helm Install failed. Please investigate."
    exit 1
  fi
}


main() {
  # These paths bypass GHCR login, host edits, MySQL and the DataWave release.
  case "${1:-}" in
    --floci-only) setup_floci; exit $? ;;
    --check-floci) setup_floci --check; exit $? ;;
    --help|-h)
      echo "Usage: $0 [--floci-only|--check-floci]"
      echo "Normal deployment: fresh Floci-backed Accumulo using local charts; existing unowned instances are rejected."
      echo "DATAWAVE_ENABLE_FLOCI=false selects HDFS-only defaults; DATAWAVE_ACCUMULO_STORAGE overrides the storage mode."
      echo "Floci options: FLOCI_NAMESPACE, FLOCI_RELEASE, FLOCI_CLIENT_NAMESPACE, FLOCI_VALUES_FILE, FLOCI_EVIDENCE_DIR."
      exit 0 ;;
    "") ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac

  # Main execution
  echo "Starting driver script for DataWave Cluster operations..."

  ready_helm_charts || return 1
  check_k8s_cluster
  set_namespace
  guard_fresh_accumulo || return 1
  if [ "$USING_MINIKUBE" = "false" ]; then
      echo "Skipping image preload since minikube cluster was not started with this deployment."
  else
      preload_docker_image rabbitmq:3.11.4-alpine
      preload_docker_image mysql:8.0.32
      preload_docker_image busybox:1.28
      preload_docker_image bitnamilegacy/zookeeper:3.6.4
      configure_etc_hosts
      update_core_dns
  fi

  configure_repository_credentials
  ghcr_login
  create_secrets
  if ! setup_mysql_operator; then
    echo "MySQL Operator setup failed. Resolve the error before deploying DataWave."
    exit 1
  fi

  FLOCI_CLIENT_NAMESPACE=$NAMESPACE
  if ! setup_floci; then
    echo "Floci setup failed. Resolve the error before deploying DataWave." >&2
    exit 1
  fi
  helm_install || return 1



  echo "Driver Script completed successfully. See kubectl get po for information"

}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
