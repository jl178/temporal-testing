#!/usr/bin/env bash
# Validate the CDK stacks against a local AWS emulator (FOSS successors to
# LocalStack, which was archived / went paid-only in March 2026).
#
#   LocalEmu   (pip install localemu; localemu start)  — full CloudFormation
#              deploy of both stacks succeeds (control-plane emulation).
#   MiniStack  (docker run -p 4566:4566 ministackorg/ministack) — network
#              stack deploys; the Temporal stack hits CFN resource types
#              MiniStack doesn't implement yet (AWS::RDS::DBSubnetGroup,
#              AWS::ServiceDiscovery::PrivateDnsNamespace,
#              AWS::EC2::SecurityGroupIngress). Its direct APIs (RDS/ECS)
#              run real containers, which CFN doesn't reach for these types.
#
# The stacks are deployed with the plain CloudFormation API (no `cdk
# bootstrap`) because the app produces no file assets and emulator CFN
# engines handle the bootstrap toolkit stack poorly.
#
# Usage: AWS_ENDPOINT_URL=http://localhost:4566 scripts/validate-emulator.sh [extra -c context...]
set -uo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1
export AWS_ENDPOINT_URL="$ENDPOINT"
export CDK_DEFAULT_ACCOUNT=000000000000 CDK_DEFAULT_REGION=us-east-1

cd "$(dirname "$0")/../infra"

if ! curl -sf -o /dev/null --max-time 5 "$ENDPOINT" && ! curl -s -o /dev/null --max-time 5 "$ENDPOINT"; then
  echo "ERROR: no emulator gateway at $ENDPOINT" >&2
  echo "Start one, e.g.:  pip install localemu && localemu start" >&2
  exit 1
fi

[ -d node_modules ] || npm install --no-fund --no-audit --loglevel=error

# CDK templates default their BootstrapVersion parameter from this SSM
# parameter; seed it instead of running the full (emulator-hostile) bootstrap.
aws ssm put-parameter --name /cdk-bootstrap/hnb659fds/version \
  --type String --value 21 --overwrite >/dev/null 2>&1 || true

echo "==> Synthesizing (natGateways=0: emulators lack AWS::EC2::EIP) $*"
npx cdk synth --quiet -c natGateways=0 "$@" || exit 1

rc=0
for stack in TemporalNetworkStack TemporalStack; do
  echo "==> Deploying $stack to $ENDPOINT"
  # Clear any earlier failed incarnation so deploy can create cleanly.
  status=$(aws cloudformation describe-stacks --stack-name "$stack" \
    --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true)
  if [ "$status" = "ROLLBACK_COMPLETE" ] || [ "$status" = "REVIEW_IN_PROGRESS" ]; then
    aws cloudformation delete-stack --stack-name "$stack"
    aws cloudformation wait stack-delete-complete --stack-name "$stack" 2>/dev/null
  fi
  if aws cloudformation deploy --stack-name "$stack" \
    --template-file "cdk.out/$stack.template.json" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM >/dev/null 2>&1; then
    count=$(aws cloudformation list-stack-resources --stack-name "$stack" \
      --query 'length(StackResourceSummaries)' --output text 2>/dev/null || echo '?')
    echo "    OK: $stack deployed ($count resources)"
  else
    echo "    FAIL: $stack — failed resource types:"
    aws cloudformation describe-stack-events --stack-name "$stack" \
      --query 'StackEvents[?contains(ResourceStatus, `FAILED`)].[ResourceType,ResourceStatusReason]' \
      --output text 2>/dev/null | sort -u | sed 's/^/      /'
    rc=1
  fi
done

if [ $rc -eq 0 ]; then
  echo "==> Stack outputs:"
  aws cloudformation describe-stacks --stack-name TemporalStack \
    --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output text | sed 's/^/    /'
fi
exit $rc
