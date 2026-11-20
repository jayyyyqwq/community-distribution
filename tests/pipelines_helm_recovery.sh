#!/usr/bin/env bash
set -euo pipefail

[[ ${GITHUB_ACTIONS:-} == true && ${GITHUB_REPOSITORY:-} == jayyyyqwq/community-distribution ]]
[[ $(kubectl config current-context) == kind-kubeflow && $KF_PROFILE == kubeflow-user-example-com ]]
[[ $STORE_CASE == retained || $STORE_CASE == replaced ]]

claim=seaweedfs-pvc
secret=mlpipeline-minio-artifact
log=$(mktemp)
trap 'rm -f "$log"' EXIT

uid() {
    kubectl get "$@" -o jsonpath='{.metadata.uid}'
}

store_volume() {
    kubectl -n kubeflow get pvc "$claim" -o jsonpath='{.spec.volumeName}'
}

credentials_digest() {
    kubectl -n "$KF_PROFILE" get secret "$secret" -o json |
        jq -ecS '.data | select(.accesskey and .secretkey)' | sha256sum
}

probe() {
    local token
    token=$(kubectl -n "$KF_PROFILE" create token default-editor) || return 1
    python3 tests/pipeline_v2_test.py verify_profile_artifacts "$token" "$KF_PROFILE" | tee "$log"
}

profile_uid=$(uid namespace "$KF_PROFILE")
secret_uid=$(uid secret "$secret" -n "$KF_PROFILE")
before_credentials=$(credentials_digest)
before_volume=$(store_volume)
before_uid=$(uid pv "$before_volume")
[[ -n $profile_uid && -n $secret_uid && -n $before_uid ]]
probe
echo 'Baseline artifact round trip passed.'

helm uninstall kubeflow-pipelines -n kubeflow --wait --timeout 10m
if [[ $STORE_CASE == replaced ]]; then
    kubectl wait --for=delete "pv/$before_volume" --timeout=180s
else
    [[ $(uid pv "$before_volume") == "$before_uid" ]]
fi
bash tests/pipelines_helm_install.sh platform-database
after_volume=$(store_volume)
after_uid=$(uid pv "$after_volume")
[[ -n $after_uid ]]
[[ $(uid namespace "$KF_PROFILE") == "$profile_uid" ]]
[[ $(uid secret "$secret" -n "$KF_PROFILE") == "$secret_uid" ]]
[[ $(credentials_digest) == "$before_credentials" ]]

if [[ $STORE_CASE == retained ]]; then
    [[ $before_uid == "$after_uid" ]]
    probe
    echo 'Retained store: original Profile credentials passed the artifact round trip.'
    exit 0
fi

[[ $before_uid != "$after_uid" ]]
if probe; then
    echo 'The replaced store accepted the original credentials; the failure did not reproduce.' >&2
    exit 1
fi
grep -Eq '^Artifact probe final state: (FAILED|ERROR)$' "$log"
run_id=$(sed -n 's/^Artifact probe run ID: //p' "$log")
[[ $run_id =~ ^[0-9a-fA-F-]{36}$ ]]
# Inspect only this run's logs; never publish credentials or raw container logs.
kubectl -n "$KF_PROFILE" logs -l "pipeline/runid=$run_id" --all-containers --tail=-1 |
    grep -E 'InvalidAccessKeyId|The access key ID you provided does not exist' > /dev/null
echo 'Replaced store: the run failed with an unrecognized access key.'

kubectl -n "$KF_PROFILE" delete secret "$secret"
kubectl -n "$KF_PROFILE" wait --for=create "secret/$secret" --timeout=180s
[[ $(uid secret "$secret" -n "$KF_PROFILE") != "$secret_uid" ]]
[[ $(credentials_digest) != "$before_credentials" ]]
probe
echo 'Replaced store: the same Profile passed the artifact round trip after recovery.'
