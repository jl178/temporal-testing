#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { DataPlaneStack } from '../lib/stacks/data-plane-stack';
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

// Console legibility: every taggable resource answers "what owns this"
// via tags even where physical names stay CloudFormation-generated
// (load balancers etc. — see docs/decisions.md on naming).
cdk.Tags.of(app).add('project', 'temporal-platform');
cdk.Tags.of(app).add('deployment', suffix ?? 'default');

const networkStack = new NetworkStack(app, name('TemporalNetworkStack'), {
  env,
  vpcId,
  natGateways: natGateways !== undefined ? Number(natGateways) : undefined,
});

// ETL data plane (S3 + EMR Serverless + Glue + optional Transfer SFTP),
// used by the aws-data-validate workflow: -c dataPlane=true plus
// emrImageUri/sftpPublicKey/etlRepo/etlTag.
const dataPlane =
  str('dataPlane') === 'true'
    ? new DataPlaneStack(app, name('TemporalDataStack'), {
        env,
        emrImageUri: str('emrImageUri'),
        sftpPublicKey: str('sftpPublicKey'),
      })
    : undefined;

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
  // -c uiAllowedCidrs=203.0.113.7/32,198.51.100.0/24 locks the public UI
  // to those ranges; omit for the open demo posture.
  uiAllowedCidrs: str('uiAllowedCidrs')?.split(',').map((c) => c.trim()),
  serviceDiscovery: str('serviceDiscovery') !== 'false',
  auroraVersion: str('auroraVersion'),
  e2eWorker:
    str('workerRepo') && str('workerTag')
      ? { repoName: str('workerRepo')!, tag: str('workerTag')! }
      : undefined,
  etlWorker:
    dataPlane && str('etlRepo') && str('etlTag')
      ? { repoName: str('etlRepo')!, tag: str('etlTag')!, dataPlane }
      : undefined,
});
