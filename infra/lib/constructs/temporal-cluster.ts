import {
  Duration,
  RemovalPolicy,
  Stack,
  aws_ec2 as ec2,
  aws_ecs as ecs,
  aws_ecs_patterns as ecs_patterns,
  aws_elasticloadbalancingv2 as elbv2,
  aws_logs as logs,
  aws_servicediscovery as servicediscovery,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { TemporalDatabase } from './temporal-database';

const DEFAULT_SERVER_IMAGE = 'temporalio/auto-setup:1.29.7';
const DEFAULT_UI_IMAGE = 'temporalio/ui:2.53.1';
const GRPC_PORT = 7233;
const HTTP_API_PORT = 7243;
const UI_PORT = 8080;

export interface TemporalClusterProps {
  readonly vpc: ec2.IVpc;
  /** Existing ECS cluster to deploy into. When omitted, one is created. */
  readonly ecsCluster?: ecs.ICluster;
  readonly database: TemporalDatabase;
  /** @default temporalio/auto-setup:1.29.7 */
  readonly serverImage?: string;
  /** @default temporalio/ui:2.53.1 */
  readonly uiImage?: string;
  /** @default 1024 */
  readonly serverCpu?: number;
  /** @default 2048 */
  readonly serverMemoryMiB?: number;
  /** Whether the UI load balancer is internet-facing. @default true */
  readonly publicUi?: boolean;
  /** Private DNS namespace for service discovery. @default temporal.local */
  readonly cloudMapNamespaceName?: string;
  /**
   * Register the server in a Cloud Map private DNS namespace. When false,
   * in-VPC clients (including the UI) use the internal NLB instead.
   * @default true
   */
  readonly serviceDiscovery?: boolean;
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
  /** Temporal HTTP API (DescribeTaskQueue etc.), reachable inside the VPC. */
  public readonly httpApiEndpoint: string;
  public readonly uiUrl: string;

  constructor(scope: Construct, id: string, props: TemporalClusterProps) {
    super(scope, id);

    // Readable physical names, prefixed with the stack name so suffixed
    // ephemeral deploys stay collision-free (see docs/decisions.md).
    const stackName = Stack.of(this).stackName;
    this.ecsCluster =
      props.ecsCluster ??
      new ecs.Cluster(this, 'EcsCluster', { vpc: props.vpc, clusterName: stackName });

    const useServiceDiscovery = props.serviceDiscovery ?? true;
    const namespaceName = props.cloudMapNamespaceName ?? 'temporal.local';
    const namespace = useServiceDiscovery
      ? new servicediscovery.PrivateDnsNamespace(this, 'Namespace', {
          name: namespaceName,
          vpc: props.vpc,
        })
      : undefined;

    // --- Temporal server ---
    const serverLogs = new logs.LogGroup(this, 'ServerLogs', {
      logGroupName: `/ecs/${stackName}/temporal-server`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const serverTask = new ecs.FargateTaskDefinition(this, 'ServerTask', {
      family: `${stackName}-temporal-server`,
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
        // Aurora always offers server TLS, and newer Postgres defaults can
        // force it; connect over TLS without pinning the RDS CA.
        POSTGRES_TLS_ENABLED: 'true',
        POSTGRES_TLS_DISABLE_HOST_VERIFICATION: 'true',
        SQL_TLS_ENABLED: 'true',
        SQL_HOST_VERIFICATION: 'false',
        SQL_TLS_DISABLE_HOST_VERIFICATION: 'true',
      },
      secrets: {
        POSTGRES_USER: ecs.Secret.fromSecretsManager(props.database.secret, 'username'),
        POSTGRES_PWD: ecs.Secret.fromSecretsManager(props.database.secret, 'password'),
      },
      portMappings: [{ containerPort: GRPC_PORT }, { containerPort: HTTP_API_PORT }],
    });

    this.serverService = new ecs.FargateService(this, 'ServerService', {
      serviceName: `${stackName}-temporal-server`,
      cluster: this.ecsCluster,
      taskDefinition: serverTask,
      desiredCount: 1,
      minHealthyPercent: 0,
      circuitBreaker: { rollback: true },
      enableECSManagedTags: true,
      propagateTags: ecs.PropagatedTagSource.SERVICE,
      cloudMapOptions: namespace
        ? {
            cloudMapNamespace: namespace,
            name: 'temporal-frontend',
            dnsRecordType: servicediscovery.DnsRecordType.A,
            dnsTtl: Duration.seconds(10),
          }
        : undefined,
    });
    props.database.allowConnectionsFrom(this.serverService);
    // NLB health checks and in-VPC SDK clients reach the task directly.
    this.serverService.connections.allowFrom(
      ec2.Peer.ipv4(props.vpc.vpcCidrBlock),
      ec2.Port.tcp(GRPC_PORT),
      'gRPC from within the VPC (NLB health checks + SDK clients)',
    );
    this.serverService.connections.allowFrom(
      ec2.Peer.ipv4(props.vpc.vpcCidrBlock),
      ec2.Port.tcp(HTTP_API_PORT),
      'Temporal HTTP API (operational tooling, backlog metrics)',
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
    this.grpcLoadBalancer
      .addListener('HttpApi', { port: HTTP_API_PORT })
      .addTargets('ServerHttp', {
        port: HTTP_API_PORT,
        targets: [
          this.serverService.loadBalancerTarget({
            containerName: 'temporal-server',
            containerPort: HTTP_API_PORT,
          }),
        ],
      });
    this.grpcEndpoint = `${this.grpcLoadBalancer.loadBalancerDnsName}:${GRPC_PORT}`;
    this.httpApiEndpoint = `http://${this.grpcLoadBalancer.loadBalancerDnsName}:${HTTP_API_PORT}`;

    // --- Temporal web UI ---
    // Explicit log group: the ecs_patterns default creates one with RETAIN,
    // which quietly outlives every stack delete (docs/gotchas.md).
    const uiLogs = new logs.LogGroup(this, 'UiLogs', {
      logGroupName: `/ecs/${stackName}/temporal-ui`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    this.uiService = new ecs_patterns.ApplicationLoadBalancedFargateService(this, 'Ui', {
      serviceName: `${stackName}-temporal-ui`,
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
        logDriver: ecs.LogDrivers.awsLogs({ streamPrefix: 'temporal-ui', logGroup: uiLogs }),
        environment: {
          TEMPORAL_ADDRESS: useServiceDiscovery
            ? `temporal-frontend.${namespaceName}:${GRPC_PORT}`
            : this.grpcEndpoint,
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

    this.uiUrl = `http://${this.uiService.loadBalancer.loadBalancerDnsName}`;
  }
}
