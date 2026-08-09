import {
  CfnOutput,
  RemovalPolicy,
  Stack,
  StackProps,
  aws_ec2 as ec2,
  aws_ecr as ecr,
  aws_ecs as ecs,
  aws_logs as logs,
  aws_route53 as route53,
  aws_secretsmanager as secretsmanager,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { TemporalCluster } from '../constructs/temporal-cluster';
import { TemporalDatabase } from '../constructs/temporal-database';
import { TemporalDns } from '../constructs/temporal-dns';
import { TemporalWorkerService } from '../constructs/temporal-worker';

export interface ExistingDatabaseConfig {
  readonly endpointAddress: string;
  readonly port?: number;
  readonly secretArn: string;
  readonly securityGroupId?: string;
}

export interface ExistingHostedZoneConfig {
  readonly hostedZoneId: string;
  readonly zoneName: string;
}

export interface TemporalStackProps extends StackProps {
  readonly vpc: ec2.IVpc;
  /** Name of an existing ECS cluster to deploy into. Omit to create one. */
  readonly ecsClusterName?: string;
  /** Existing Postgres database. Omit to create Aurora Serverless v2. */
  readonly existingDatabase?: ExistingDatabaseConfig;
  /** Enables DNS: records are created under this domain. Omit to skip DNS. */
  readonly domainName?: string;
  /** Existing hosted zone for domainName. Omit to create a zone (when domainName is set). */
  readonly existingHostedZone?: ExistingHostedZoneConfig;
  /** @default true */
  readonly publicUi?: boolean;
  /** Cloud Map service discovery for the server. @default true */
  readonly serviceDiscovery?: boolean;
  /**
   * Workflow-execution e2e harness: a worker fleet + a one-shot starter
   * task definition using a pre-pushed image from ECR. Used by the
   * aws-deploy-validate workflow; omit for normal deployments.
   */
  readonly e2eWorker?: { repoName: string; tag: string };
}

/**
 * The full Temporal deployment: database (create or import), server + UI on
 * Fargate (cluster create or import), and optional DNS (zone create or import).
 */
export class TemporalStack extends Stack {
  public readonly database: TemporalDatabase;
  public readonly temporal: TemporalCluster;
  public readonly dns?: TemporalDns;

  constructor(scope: Construct, id: string, props: TemporalStackProps) {
    super(scope, id, props);

    this.database = new TemporalDatabase(this, 'Database', {
      vpc: props.vpc,
      existingDatabase: props.existingDatabase
        ? {
            endpointAddress: props.existingDatabase.endpointAddress,
            port: props.existingDatabase.port,
            secret: secretsmanager.Secret.fromSecretCompleteArn(
              this,
              'ImportedDbSecret',
              props.existingDatabase.secretArn,
            ),
            securityGroup: props.existingDatabase.securityGroupId
              ? ec2.SecurityGroup.fromSecurityGroupId(
                  this,
                  'ImportedDbSg',
                  props.existingDatabase.securityGroupId,
                )
              : undefined,
          }
        : undefined,
    });

    this.temporal = new TemporalCluster(this, 'Temporal', {
      vpc: props.vpc,
      ecsCluster: props.ecsClusterName
        ? ecs.Cluster.fromClusterAttributes(this, 'ImportedEcsCluster', {
            clusterName: props.ecsClusterName,
            vpc: props.vpc,
          })
        : undefined,
      database: this.database,
      publicUi: props.publicUi,
      serviceDiscovery: props.serviceDiscovery,
    });

    if (props.domainName) {
      this.dns = new TemporalDns(this, 'Dns', {
        domainName: props.domainName,
        hostedZone: props.existingHostedZone
          ? route53.HostedZone.fromHostedZoneAttributes(this, 'ImportedZone', {
              hostedZoneId: props.existingHostedZone.hostedZoneId,
              zoneName: props.existingHostedZone.zoneName,
            })
          : undefined,
      });
      this.dns.addUiRecord(this.temporal.uiService.loadBalancer);
      this.dns.addGrpcRecord(this.temporal.grpcLoadBalancer);
    }

    if (props.e2eWorker) {
      const repo = ecr.Repository.fromRepositoryName(
        this, 'E2eRepo', props.e2eWorker.repoName,
      );
      const image = ecs.ContainerImage.fromEcrRepository(repo, props.e2eWorker.tag);

      const worker = new TemporalWorkerService(this, 'E2eWorker', {
        ecsCluster: this.temporal.ecsCluster,
        image,
        temporalAddress: this.temporal.grpcEndpoint,
        taskQueue: 'greeting-tasks-python',
      });
      worker.allowGrpcTo(this.temporal.serverService);

      const starterLogs = new logs.LogGroup(this, 'E2eStarterLogs', {
        logGroupName: `/ecs/${this.stackName}/e2e-starter`,
        retention: logs.RetentionDays.ONE_DAY,
        removalPolicy: RemovalPolicy.DESTROY,
      });
      const starterTask = new ecs.FargateTaskDefinition(this, 'E2eStarterTask', {
        family: `${this.stackName}-e2e-starter`,
        cpu: 256,
        memoryLimitMiB: 512,
      });
      starterTask.addContainer('starter', {
        image,
        command: ['python', 'starter.py'],
        environment: {
          TEMPORAL_ADDRESS: this.temporal.grpcEndpoint,
          TEMPORAL_NAMESPACE: 'default',
        },
        logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'e2e-starter', logGroup: starterLogs }),
      });
      const starterSg = new ec2.SecurityGroup(this, 'E2eStarterSg', {
        vpc: props.vpc,
        allowAllOutbound: true,
      });

      new CfnOutput(this, 'E2eClusterName', { value: this.temporal.ecsCluster.clusterName });
      new CfnOutput(this, 'E2eStarterTaskDef', { value: starterTask.taskDefinitionArn });
      new CfnOutput(this, 'E2eStarterSecurityGroup', { value: starterSg.securityGroupId });
      new CfnOutput(this, 'E2ePrivateSubnets', {
        value: props.vpc.privateSubnets.map((s) => s.subnetId).join(','),
      });
      new CfnOutput(this, 'E2eStarterLogGroup', { value: starterLogs.logGroupName });
    }

    new CfnOutput(this, 'GrpcEndpoint', {
      value: this.dns ? `${this.dns.grpcDomainName}:7233` : this.temporal.grpcEndpoint,
      description: 'Temporal frontend gRPC endpoint (reachable from inside the VPC)',
    });
    new CfnOutput(this, 'UiUrl', {
      value: this.dns ? `http://${this.dns.uiDomainName}` : this.temporal.uiUrl,
      description: 'Temporal web UI',
    });
    new CfnOutput(this, 'DatabaseSecretArn', {
      value: this.database.secret.secretArn,
      description: 'Secrets Manager secret holding database credentials',
    });
  }
}
