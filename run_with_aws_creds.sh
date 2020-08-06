#!/usr/bin/env bash

readonly security_credentials_url='http://169.254.169.254/latest/meta-data/iam/security-credentials'
readonly role=$(curl -s "${security_credentials_url}/")
readonly response=$(curl -s "${security_credentials_url}/${role}")

readonly code=$(echo "$response" | jq '.Code' -r)

[[ "Success" == "$code" ]] || {
  echo "Error !!! Code is $code"
  exit 1
}

readonly AWS_ACCESS_KEY_ID=$(echo "$response" | jq '.AccessKeyId' -r)
readonly AWS_SECRET_ACCESS_KEY=$(echo "$response" | jq '.SecretAccessKey' -r)
readonly AWS_SESSION_TOKEN=$(echo "$response" | jq '.Token' -r)

export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN
export SERVICE_REGION='us-west-2'

echo "$AWS_ACCESS_KEY_ID"
echo "$AWS_SECRET_ACCESS_KEY"
echo "$AWS_SESSION_TOKEN"
echo "$SERVICE_REGION"

gradle jar

readonly neptune="$1"
readonly type="$2"
readonly FETCH_SIZE='500'
readonly POOL_SIZE='8'
java -jar build/libs/neptune-dumper-1.0-SNAPSHOT.jar \
  "$neptune" "$type" "$FETCH_SIZE" "$POOL_SIZE"
