import { App, Stack, aws_ec2 as ec2 } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { TemporalStack } from '../lib/stacks/temporal-stack';
import { NetworkStack } from '../lib/stacks/network-stack';

const ENV = { account: '111111111111', region: 'us-east-1' };

function importedVpc(scope: Stack): ec2.IVpc {
  return ec2.Vpc.fromVpcAttributes(scope, 'ExistingVpc', {
    vpcId: 'vpc-12345678',
    vpcCidrBlock: '10.100.0.0/16',
    availabilityZones: ['us-east-1a', 'us-east-1b'],
    publicSubnetIds: ['subnet-pub1', 'subnet-pub2'],
    privateSubnetIds: ['subnet-priv1', 'subnet-priv2'],
  });
}

describe('create-everything mode', () => {
  const app = new App();
  const network = new NetworkStack(app, 'Net', { env: ENV });
  const stack = new TemporalStack(app, 'Temporal', { env: ENV, vpc: network.vpc });
  const netTemplate = Template.fromStack(network);
  const template = Template.fromStack(stack);

  test('creates a 2-AZ VPC with one NAT gateway', () => {
    netTemplate.resourceCountIs('AWS::EC2::VPC', 1);
    netTemplate.resourceCountIs('AWS::EC2::NatGateway', 1);
    netTemplate.resourceCountIs('AWS::EC2::Subnet', 4);
  });

  test('creates an Aurora Serverless v2 PostgreSQL cluster with generated secret', () => {
    template.hasResourceProperties('AWS::RDS::DBCluster', {
      Engine: 'aurora-postgresql',
      ServerlessV2ScalingConfiguration: { MinCapacity: 0.5, MaxCapacity: 4 },
      DatabaseName: 'temporal',
    });
    template.hasResourceProperties('AWS::RDS::DBInstance', {
      DBInstanceClass: 'db.serverless',
    });
    template.resourceCountIs('AWS::SecretsManager::Secret', 1);
  });

  test('creates an ECS cluster and both Fargate services', () => {
    template.resourceCountIs('AWS::ECS::Cluster', 1);
    template.resourceCountIs('AWS::ECS::Service', 2);
    template.hasResourceProperties('AWS::ECS::TaskDefinition', {
      ContainerDefinitions: Match.arrayWith([
        Match.objectLike({
          Image: 'temporalio/auto-setup:1.27.2',
          Environment: Match.arrayWith([
            { Name: 'DB', Value: 'postgres12' },
            { Name: 'DB_PORT', Value: '5432' },
          ]),
          Secrets: Match.arrayWith([
            Match.objectLike({ Name: 'POSTGRES_USER' }),
            Match.objectLike({ Name: 'POSTGRES_PWD' }),
          ]),
        }),
      ]),
    });
    template.hasResourceProperties('AWS::ECS::TaskDefinition', {
      ContainerDefinitions: Match.arrayWith([
        Match.objectLike({ Image: 'temporalio/ui:2.36.0' }),
      ]),
    });
  });

  test('exposes gRPC via an internal NLB and the UI via an internet-facing ALB', () => {
    template.hasResourceProperties('AWS::ElasticLoadBalancingV2::LoadBalancer', {
      Type: 'network',
      Scheme: 'internal',
    });
    template.hasResourceProperties('AWS::ElasticLoadBalancingV2::LoadBalancer', {
      Type: 'application',
      Scheme: 'internet-facing',
    });
    template.hasResourceProperties('AWS::ElasticLoadBalancingV2::Listener', {
      Port: 7233,
      Protocol: 'TCP',
    });
  });

  test('registers the server in a Cloud Map private DNS namespace', () => {
    template.hasResourceProperties('AWS::ServiceDiscovery::PrivateDnsNamespace', {
      Name: 'temporal.local',
    });
    template.hasResourceProperties('AWS::ServiceDiscovery::Service', {
      Name: 'temporal-frontend',
    });
  });

  test('creates no DNS resources when domainName is omitted', () => {
    template.resourceCountIs('AWS::Route53::HostedZone', 0);
    template.resourceCountIs('AWS::Route53::RecordSet', 0);
  });
});

describe('import-existing mode', () => {
  const app = new App();
  const shell = new Stack(app, 'Shell', { env: ENV });
  const stack = new TemporalStack(app, 'Temporal', {
    env: ENV,
    vpc: importedVpc(shell),
    ecsClusterName: 'existing-cluster',
    existingDatabase: {
      endpointAddress: 'existing-db.cluster-abc.us-east-1.rds.amazonaws.com',
      secretArn:
        'arn:aws:secretsmanager:us-east-1:111111111111:secret:existing-db-secret-AbCdEf',
      securityGroupId: 'sg-99999999',
    },
    existingHostedZone: { hostedZoneId: 'Z0000000000000000000A', zoneName: 'example.com' },
    domainName: 'temporal.example.com',
    publicUi: false,
  });
  const template = Template.fromStack(stack);

  test('creates no VPC, ECS cluster, database, secret, or hosted zone', () => {
    template.resourceCountIs('AWS::EC2::VPC', 0);
    template.resourceCountIs('AWS::ECS::Cluster', 0);
    template.resourceCountIs('AWS::RDS::DBCluster', 0);
    template.resourceCountIs('AWS::RDS::DBInstance', 0);
    template.resourceCountIs('AWS::SecretsManager::Secret', 0);
    template.resourceCountIs('AWS::Route53::HostedZone', 0);
  });

  test('points the server at the existing database endpoint', () => {
    template.hasResourceProperties('AWS::ECS::TaskDefinition', {
      ContainerDefinitions: Match.arrayWith([
        Match.objectLike({
          Environment: Match.arrayWith([
            {
              Name: 'POSTGRES_SEEDS',
              Value: 'existing-db.cluster-abc.us-east-1.rds.amazonaws.com',
            },
          ]),
        }),
      ]),
    });
  });

  test('opens ingress on the existing database security group', () => {
    template.hasResourceProperties('AWS::EC2::SecurityGroupIngress', {
      GroupId: 'sg-99999999',
      FromPort: 5432,
      ToPort: 5432,
    });
  });

  test('adds alias records to the existing hosted zone', () => {
    template.hasResourceProperties('AWS::Route53::RecordSet', {
      Name: 'ui.temporal.example.com.',
      Type: 'A',
      HostedZoneId: 'Z0000000000000000000A',
    });
    template.hasResourceProperties('AWS::Route53::RecordSet', {
      Name: 'grpc.temporal.example.com.',
      Type: 'A',
      HostedZoneId: 'Z0000000000000000000A',
    });
  });

  test('keeps the UI load balancer internal', () => {
    template.hasResourceProperties('AWS::ElasticLoadBalancingV2::LoadBalancer', {
      Type: 'application',
      Scheme: 'internal',
    });
  });
});

describe('create-zone mode', () => {
  const app = new App();
  const shell = new Stack(app, 'Shell', { env: ENV });
  const stack = new TemporalStack(app, 'Temporal', {
    env: ENV,
    vpc: importedVpc(shell),
    domainName: 'temporal.example.com',
  });
  const template = Template.fromStack(stack);

  test('creates a hosted zone when domainName is set without an existing zone', () => {
    template.hasResourceProperties('AWS::Route53::HostedZone', {
      Name: 'temporal.example.com.',
    });
    template.resourceCountIs('AWS::Route53::RecordSet', 2);
  });
});
