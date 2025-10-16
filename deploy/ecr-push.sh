#!/bin/sh

if [ $# -lt 1 ]
then
    echo "Usage: ecr-push.sh <env>"
    exit 1
fi

source deploy/env.sh env-common.list
source deploy/env.sh env-$1.list

# AWS CLI v2
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_HOST

docker buildx build --platform linux/arm64 --push \
    -t $ECR_HOST/$IMAGE_NAME .
