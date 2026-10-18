#!/usr/bin/env bash
set -euo pipefail
[[ "$ACCUMULO_INSTANCE_NAME" == floci-lab ]]
[[ "$LAB_HDFS_VOLUME" == hdfs://hdfs-nn.default.svc.cluster.local:9000/accumulo-s3-lab ]]
[[ "$LAB_S3_VOLUME" == s3a://dwv-accumulo-lab/accumulo ]]
mkdir -p "$ACCUMULO_CONF_DIR"
cp /source-accumulo/* "$ACCUMULO_CONF_DIR/"
chmod -R u+rw "$ACCUMULO_CONF_DIR"
inspect_state() {
    accumulo local.floci.LabState "$ACCUMULO_INSTANCE_NAME" "$LAB_ZOOKEEPERS" "$LAB_HDFS_VOLUME" "$LAB_S3_VOLUME" > /work/state.log
    cat /work/state.log
    STATE=$(awk -F= '/^STATE=/{state=$2} END{print state}' /work/state.log)
    [[ -n "$STATE" ]]
}
inspect_state
if [[ "$STATE" == ABSENT ]]; then
    sed -i "s|^instance.volumes=.*|instance.volumes=$LAB_HDFS_VOLUME|" "$ACCUMULO_CONF_DIR/accumulo.properties"
    hdfs dfs -mkdir -p "$LAB_HDFS_VOLUME"
    hdfs dfs -chmod 700 "$LAB_HDFS_VOLUME"
    accumulo init --instance-name "$ACCUMULO_INSTANCE_NAME" --password "$ACCUMULO_ROOT_PASSWORD"
    inspect_state
    [[ "$STATE" == INITIALIZED ]]
fi
# A partial or mismatched state fails in LabState; it is never silently reinitialized.
sed -i "s|^instance.volumes=.*|instance.volumes=$LAB_HDFS_VOLUME,$LAB_S3_VOLUME|" "$ACCUMULO_CONF_DIR/accumulo.properties"
if [[ "$STATE" == INITIALIZED ]]; then
    accumulo init --add-volumes
fi
inspect_state
[[ "$STATE" == REGISTERED ]]
printf 'LAB_VOLUME_BOOTSTRAP=PASS\n'
