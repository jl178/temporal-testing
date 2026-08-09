#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { NetworkStack } from '../lib/stacks/network-stack';
import { TemporalStack } from '../lib/stacks/temporal-stack';

/**
 * Everything "might already exist": pass context to import instead of create.
 *
 *   -c vpcId=vpc-0123...            reuse an existing VPC (needs account/region)
 *   -c ecsClusterName=my-cluster    reuse an existing ECS cluster
 *   -c dbEndpoint=host -c dbSecretArn=arn:...  reuse an existing Postgres
 *   -c dbSecurityGroupId=sg-0123... let the stack open ingress on the existing DB
 *   -c domainName=temporal.example.com          enable DNS records
 *   -c hostedZoneId=Z... -c zoneName=example.com  reuse an existing zone
 *   -c publicUi=false               keep the UI load balancer internal
 */
const app = new cdk.App();

const env: cdk.Environment = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION ?? 'us-east-1',
};

const str = (key: string): string | undefined => app.node.tryGetContext(key);

const vpcId = str('vpcId');
const dbEndpoint = str('dbEndpoint');
const dbSecretArn = str('dbSecretArn');
const hostedZoneId = str('hostedZoneId');
const zoneName = str('zoneName');

const natGateways = str('natGateways');
// Optional suffix so ephemeral deploys (CI validation) are guaranteed
// net-new and can never collide with or update existing stacks.
const suffix = str('stackSuffix');
const name = (base: string) => (suffix ? `${base}-${suffix}` : base);

const networkStack = new NetworkStack(app, name('TemporalNetworkStack'), {
  env,
  vpcId,
  natGateways: natGateways !== undefined ? Number(natGateways) : undefined,
});

new TemporalStack(app, name('TemporalStack'), {
  env,
  vpc: networkStack.vpc,
  ecsClusterName: str('ecsClusterName'),
  existingDatabase:
    dbEndpoint && dbSecretArn
      ? {
          endpointAddress: dbEndpoint,
          secretArn: dbSecretArn,
          securityGroupId: str('dbSecurityGroupId'),
        }
      : undefined,
  domainName: str('domainName'),
  existingHostedZone:
    hostedZoneId && zoneName ? { hostedZoneId, zoneName } : undefined,
  publicUi: str('publicUi') !== 'false',
  serviceDiscovery: str('serviceDiscovery') !== 'false',
});
