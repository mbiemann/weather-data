#!/bin/bash
set -e
echo ""

# Inputs
ENVIRONMENT=$1
INFRA_BUCKET=$2
[[ -z "$ENVIRONMENT" ]] && { echo "ERROR: ENVIRONMENT is not set. Use ./aws-cfstack-deploy.sh <environment> <bucket>"; exit 1; }
[[ -z "$INFRA_BUCKET" ]] && { echo "ERROR: INFRA_BUCKET is not set. Use ./aws-cfstack-deploy.sh <environment> <bucket>"; exit 1; }

# Parameters
STACK_NAME=cfstack-$ENVIRONMENT-weather-data

################################################################################

echo "AWS CloudFormation Stack Deploy @ $ENVIRONMENT"
echo ""
echo "Start"
echo ""

########## Package #############################################################

echo "CloudFormation Packaging..."
echo "  Template File = aws-cfstack-template.yaml"
echo ""

aws cloudformation package --template-file aws-cfstack-template.yaml \
    --output-template-file aws-cfstack-template_pkgd.yaml \
    --s3-bucket $INFRA_BUCKET

echo ""
echo "CloudFormation Package successfully!"
echo ""

########## Deploy ##############################################################

echo "CloudFormation Deploying..."
echo "  Stack Name = $STACK_NAME"
echo ""

aws cloudformation deploy --template-file aws-cfstack-template_pkgd.yaml \
    --stack-name $STACK_NAME \
    --s3-bucket $INFRA_BUCKET \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
    --no-fail-on-empty-changeset \
    --parameter-overrides Environment=$ENVIRONMENT

echo ""
echo "CloudFormation Deploy successfully!"
echo ""

########## Outputs##############################################################

OUTPUTS_JSON=aws-cfstack-outputs.json

echo "Getting CloudFormation Outputs..."
echo "  Outputs Json Filename = $OUTPUTS_JSON"
echo ""

aws cloudformation describe-stacks --stack-name $STACK_NAME | \
    jq -r '.Stacks[0].Outputs' > $OUTPUTS_JSON ; cat $OUTPUTS_JSON

BUCKET_NAME=$(jq -r '.[] | select(.OutputKey == "BucketName") | .OutputValue' $OUTPUTS_JSON)
echo "  Bucket Name = $BUCKET_NAME"

echo ""
echo "Get CloudFormation Outputs successfully!"
echo ""

################################################################################

echo "Copying batch artifacts to bucket..."
echo ""

aws s3 cp ./aws-batch/glue_script.py s3://$BUCKET_NAME/batch-artifacts/glue_script.py

echo ""

################################################################################

echo "Copying test data to bucket..."
echo ""

aws s3 cp ./test-data/ s3://$BUCKET_NAME/event-incoming/ --recursive

aws s3 cp ./test-data/ s3://$BUCKET_NAME/batch-incoming/ --recursive

echo ""

################################################################################

echo "End"
echo ""