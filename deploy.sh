#!/bin/bash 
echo hello world

while getopts "r:b:e:" opt
do
   case "$opt" in
      r) REPO="$OPTARG" ;;
      b) BRANCH="$OPTARG" ;;
      e) ENV="$OPTARG" ;;
    #   c ) COMMIT="$OPTARG" ;;
   esac
done

S3_PREFIX="${REPO}/${BRANCH}/${ENV}"
echo $S3_PREFIX
sam build  --template cfn-template.yml  && sam package --s3-bucket colin-packages --s3-prefix $S3_PREFIX --output-template-file cfn-packaged.yaml --region us-east-1 
echo "INFO: Packaged lambda ${FUNCTION} to ${S3_PREFIX}"
sam deploy --template-file cfn-packaged.yaml --s3-bucket colin-packages --s3-prefix $S3_PREFIX --stack-name ${REPO}-${ENV}-stack --capabilities CAPABILITY_NAMED_IAM --region us-east-1 --parameter-overrides Environment=$ENV 
echo "INFO: Deployed stack $REPO-stack" 