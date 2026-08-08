import {
  Duration,
  RemovalPolicy,
  aws_ec2 as ec2,
  aws_ecs as ecs,
  aws_ecs_patterns as ecs_patterns,
  aws_elasticloadbalancingv2 as elbv2,
  aws_logs as logs,
  aws_servicediscovery as servicediscovery,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { TemporalDatabase } from './temporal-database';

const DEFAULT_SERVER_IMAGE = 'temporalio/auto-setup:1.27.2';
const DEFAULT_UI_IMAGE = 'temporalio/ui:2.36.0';
const GRPC_PORT = 7233;
const UI_PORT = 8080;

export interface TemporalClusterProps {
  readonly vpc: ec2.IVpc;
  /** Existing ECS cluster to deploy into. When omitted, one is created. */
  readonly ecsCluster?: ecs.ICluster;
  readonly database: TemporalDatabase;
  /** @default temporalio/auto-setup:1.27.2 */
  readonly serverImage?: string;
  /** @default temporalio/ui:2.36.0 */
  readonly uiImage?: string;
  /** @default 1024 */
  readonly serverCpu?: number;
  /** @default 2048 */
  readonly serverMemoryMiB?: number;
  /** Whether the UI load balancer is internet-facing. @default true */
  readonly publicUi?: boolean;
  /** Private DNS namespace for service discovery. @default temporal.local */
  readonly cloudMapNamespaceName?: string;
}

/**
 * Temporal server (auto-setup image: all four services in one container, and
 * it creates/migrates the database schema on boot) plus the web UI, both on
 * Fargate. gRPC is exposed through an internal NLB and via Cloud Map DNS
 * (temporal-frontend.<namespace>); the UI sits behind an ALB.
 */
export class TemporalCluster extends Construct {
  public readonly ecsCluster: ecs.ICluster;
  public readonly serverService: ecs.FargateService;
  public readonly uiService: ecs_patterns.ApplicationLoadBalancedFargateService;
  public readonly grpcLoadBalancer: elbv2.NetworkLoadBalancer;
  /** host:port for SDK clients inside the VPC. */
  public readonly grpcEndpoint: string;
  public readonly uiUrl: string;

  constructor(scope: Construct, id: string, props: TemporalClusterProps) {
    super(scope, id);

    this.ecsCluster =
      props.ecsCluster ?? new ecs.Cluster(this, 'EcsCluster', { vpc: props.vpc });

    const namespaceName = props.cloudMapNamespaceName ?? 'temporal.local';
    const namespace = new servicediscovery.PrivateDnsNamespace(this, 'Namespace', {
      name: namespaceName,
      vpc: props.vpc,
    });

    // --- Temporal server ---
    const serverLogs = new logs.LogGroup(this, 'ServerLogs', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const serverTask = new ecs.FargateTaskDefinition(this, 'ServerTask', {
      cpu: props.serverCpu ?? 1024,
      memoryLimitMiB: props.serverMemoryMiB ?? 2048,
    });
    serverTask.addContainer('temporal-server', {
      image: ecs.ContainerImage.fromRegistry(props.serverImage ?? DEFAULT_SERVER_IMAGE),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'temporal-server', logGroup: serverLogs }),
      environment: {
        DB: 'postgres12',
        DB_PORT: String(props.database.port),
        POSTGRES_SEEDS: props.database.endpointAddress,
      },
      secrets: {
        POSTGRES_USER: ecs.Secret.fromSecretsManager(props.database.secret, 'username'),
        POSTGRES_PWD: ecs.Secret.fromSecretsManager(props.database.secret, 'password'),
      },
      portMappings: [{ containerPort: GRPC_PORT }],
    });

    this.serverService = new ecs.FargateService(this, 'ServerService', {
      cluster: this.ecsCluster,
      taskDefinition: serverTask,
      desiredCount: 1,
      minHealthyPercent: 0,
      circuitBreaker: { rollback: true },
      cloudMapOptions: {
        cloudMapNamespace: namespace,
        name: 'temporal-frontend',
        dnsRecordType: servicediscovery.DnsRecordType.A,
        dnsTtl: Duration.seconds(10),
      },
    });
    props.database.allowConnectionsFrom(this.serverService);
    // NLB health checks and in-VPC SDK clients reach the task directly.
    this.serverService.connections.allowFrom(
      ec2.Peer.ipv4(props.vpc.vpcCidrBlock),
      ec2.Port.tcp(GRPC_PORT),
      'gRPC from within the VPC (NLB health checks + SDK clients)',
    );

    // Internal NLB so clients outside the Cloud Map namespace (e.g. peered
    // networks, VPN) get a stable gRPC endpoint.
    this.grpcLoadBalancer = new elbv2.NetworkLoadBalancer(this, 'GrpcLb', {
      vpc: props.vpc,
      internetFacing: false,
    });
    this.grpcLoadBalancer
      .addListener('Grpc', { port: GRPC_PORT })
      .addTargets('Server', { port: GRPC_PORT, targets: [this.serverService] });

    // --- Temporal web UI ---
    this.uiService = new ecs_patterns.ApplicationLoadBalancedFargateService(this, 'Ui', {
      cluster: this.ecsCluster,
      cpu: 256,
      memoryLimitMiB: 512,
      desiredCount: 1,
      minHealthyPercent: 0,
      circuitBreaker: { rollback: true },
      publicLoadBalancer: props.publicUi ?? true,
      taskImageOptions: {
        image: ecs.ContainerImage.fromRegistry(props.uiImage ?? DEFAULT_UI_IMAGE),
        containerPort: UI_PORT,
        environment: {
          TEMPORAL_ADDRESS: `temporal-frontend.${namespaceName}:${GRPC_PORT}`,
          TEMPORAL_UI_PORT: String(UI_PORT),
        },
      },
    });
    this.uiService.targetGroup.configureHealthCheck({ path: '/', healthyHttpCodes: '200' });
    this.serverService.connections.allowFrom(
      this.uiService.service,
      ec2.Port.tcp(GRPC_PORT),
      'UI to Temporal frontend',
    );

    this.grpcEndpoint = `${this.grpcLoadBalancer.loadBalancerDnsName}:${GRPC_PORT}`;
    this.uiUrl = `http://${this.uiService.loadBalancer.loadBalancerDnsName}`;
  }
}
