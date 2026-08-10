import { App, Stack, aws_ec2 as ec2, aws_ecs as ecs } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { TemporalStack } from '../lib/stacks/temporal-stack';
import { NetworkStack } from '../lib/stacks/network-stack';
import { TemporalWorkerService } from '../lib/constructs/temporal-worker';

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
          Image: Match.stringLikeRegexp('temporalio/auto-setup:.*'),
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
        Match.objectLike({ Image: 'temporalio/ui:2.53.1' }),
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

describe('public UI with CIDR allowlist', () => {
  const app = new App();
  const shell = new Stack(app, 'Shell', { env: ENV });
  const stack = new TemporalStack(app, 'Temporal', {
    env: ENV,
    vpc: importedVpc(shell),
    uiAllowedCidrs: ['203.0.113.7/32', '198.51.100.0/24'],
  });
  const template = Template.fromStack(stack);

  test('grants ingress only to the allowlisted CIDRs, never 0.0.0.0/0', () => {
    for (const cidr of ['203.0.113.7/32', '198.51.100.0/24']) {
      template.hasResourceProperties('AWS::EC2::SecurityGroup', {
        SecurityGroupIngress: Match.arrayWith([
          Match.objectLike({ CidrIp: cidr, FromPort: 80, IpProtocol: 'tcp' }),
        ]),
      });
    }
    const sgs = template.findResources('AWS::EC2::SecurityGroup');
    for (const sg of Object.values(sgs)) {
      for (const rule of (sg as any).Properties?.SecurityGroupIngress ?? []) {
        expect(rule.CidrIp).not.toBe('0.0.0.0/0');
      }
    }
  });
});

describe('service discovery disabled', () => {
  const app = new App();
  const shell = new Stack(app, 'Shell', { env: ENV });
  const stack = new TemporalStack(app, 'Temporal', {
    env: ENV,
    vpc: importedVpc(shell),
    serviceDiscovery: false,
  });
  const template = Template.fromStack(stack);

  test('creates no Cloud Map resources and points the UI at the NLB', () => {
    template.resourceCountIs('AWS::ServiceDiscovery::PrivateDnsNamespace', 0);
    template.resourceCountIs('AWS::ServiceDiscovery::Service', 0);
    const taskDefs = template.findResources('AWS::ECS::TaskDefinition');
    const uiDef = Object.values(taskDefs).find((td: any) =>
      JSON.stringify(td).includes('temporalio/ui'),
    ) as any;
    const env = uiDef.Properties.ContainerDefinitions[0].Environment;
    const address = env.find((e: any) => e.Name === 'TEMPORAL_ADDRESS');
    // Address is a CFN join over the NLB's DNSName attribute, not a Cloud Map name.
    expect(JSON.stringify(address.Value)).toContain('DNSName');
  });
});

describe('autoscaled worker fleet', () => {
  const app = new App();
  const stack = new Stack(app, 'Workers', { env: ENV });
  const vpc = importedVpc(stack);
  const cluster = new ecs.Cluster(stack, 'EcsCluster', { vpc });
  new TemporalWorkerService(stack, 'HeavyFleet', {
    ecsCluster: cluster,
    image: ecs.ContainerImage.fromRegistry('example/etl-heavy-worker:1'),
    command: [
      'python', '-m', 'worker_platform',
      '--queue', 'compute-large', '--profile', 'large',
      '--activities', 'activities:run_local_transform',
    ],
    temporalAddress: 'temporal-frontend.temporal.local:7233',
    taskQueue: 'compute-large',
    profile: 'large',
    autoscaling: {
      maxCapacity: 10,
      scaleUpBacklog: 50,
      temporalHttpEndpoint: 'http://internal-nlb.example:7243',
    },
  });
  const template = Template.fromStack(stack);

  test('publishes backlog metrics via a scheduled poller lambda', () => {
    template.hasResourceProperties('AWS::Lambda::Function', {
      Runtime: 'python3.12',
      Environment: {
        Variables: Match.objectLike({
          TASK_QUEUE: 'compute-large',
          TEMPORAL_HTTP_ENDPOINT: 'http://internal-nlb.example:7243',
        }),
      },
    });
    template.hasResourceProperties('AWS::Events::Rule', {
      ScheduleExpression: 'rate(1 minute)',
    });
  });

  test('step-scales the service on backlog depth', () => {
    template.hasResourceProperties('AWS::ApplicationAutoScaling::ScalableTarget', {
      MinCapacity: 1,
      MaxCapacity: 10,
      ServiceNamespace: 'ecs',
    });
    template.hasResourceProperties('AWS::ApplicationAutoScaling::ScalingPolicy', {
      PolicyType: 'StepScaling',
    });
    template.hasResourceProperties('AWS::CloudWatch::Alarm', {
      Namespace: 'Temporal/TaskQueue',
      MetricName: 'ApproximateBacklogCount',
      Dimensions: Match.arrayWith([
        Match.objectLike({ Name: 'TaskQueue', Value: 'compute-large' }),
      ]),
    });
  });

  test('alarms when the oldest task waits over five minutes', () => {
    template.hasResourceProperties('AWS::CloudWatch::Alarm', {
      MetricName: 'ApproximateBacklogAgeSeconds',
      Threshold: 300,
      EvaluationPeriods: 5,
      TreatMissingData: 'notBreaching',
    });
  });

  test('worker container knows its queue, namespace, command, and profile size', () => {
    template.hasResourceProperties('AWS::ECS::TaskDefinition', {
      Cpu: '4096',
      Memory: '16384',
      ContainerDefinitions: Match.arrayWith([
        Match.objectLike({
          Command: Match.arrayWith(['worker_platform', '--profile', 'large']),
          Environment: Match.arrayWith([
            { Name: 'TEMPORAL_NAMESPACE', Value: 'default' },
            { Name: 'TEMPORAL_TASK_QUEUE', Value: 'compute-large' },
          ]),
        }),
      ]),
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
